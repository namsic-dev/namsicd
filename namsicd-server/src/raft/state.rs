use std::{cmp, collections::HashMap, fmt, ops::Range, sync::Arc};

use rand::Rng;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time,
};

use crate::{
    Operation,
    raft::pb::{RequestVote, ResponseAppendEntries, ResponseVote, raft_message::Content},
};

use super::{
    CHAN_BUF_TODO, Event, RaftError, grpc,
    log::Log,
    pb::{RaftMessage, RequestAppendEntries},
};

#[derive(Default)]
#[cfg_attr(test, derive(Debug, PartialEq))]
enum State {
    #[default]
    Follower,
    Candidate,
    Leader,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Follower => "follower",
                Self::Candidate => "candidate",
                Self::Leader => "leader",
            }
        )
    }
}

struct MemberInfo {
    _addr: String,
    tx: mpsc::Sender<RaftMessage>,
    next_index: u64,
    match_index: u64,
}

struct ApplyStatus {
    last_applied: u64,
    waiter: HashMap<u64, oneshot::Sender<Result<Vec<u8>, RaftError>>>,
}

async fn consume_committed_rx(
    mut committed_rx: mpsc::Receiver<Vec<u8>>,
    operation_tx: mpsc::Sender<Operation>,
    applied_tx: mpsc::Sender<oneshot::Receiver<Vec<u8>>>,
) {
    while let Some(data) = committed_rx.recv().await {
        let (result_tx, result_rx) = oneshot::channel();
        operation_tx
            .send(Operation { data, result_tx })
            .await
            .unwrap();
        applied_tx.send(result_rx).await.unwrap();
    }
}

async fn consume_applied_rx(
    mut applied_rx: mpsc::Receiver<oneshot::Receiver<Vec<u8>>>,
    mutex: Arc<Mutex<ApplyStatus>>,
) {
    while let Some(rx) = applied_rx.recv().await {
        let Ok(result) = rx.await else {
            unimplemented!("committed logs must be applied")
        };
        let mut apply_status = mutex.lock().await;
        apply_status.last_applied += 1;
        let last_applied = apply_status.last_applied;
        if let Some(waiter) = apply_status.waiter.remove(&last_applied) {
            let _ = waiter.send(Ok(result));
        }
    }
}

pub struct StateMachine {
    id: u64,
    election_timeout_range: Range<u64>,

    /// log entries
    log: Log,
    /// index of highest log entry known to be committed
    commit_index: u64,
    committed_tx: mpsc::Sender<Vec<u8>>,
    apply_status: Arc<Mutex<ApplyStatus>>,

    state: State,
    member_info: HashMap<u64, MemberInfo>,
    election_elapsed: u64,
    election_timeout: u64,

    /// latest term server has seen
    current_term: u64,
    /// candidateId that received vote in current term
    voted_for: u64,
    vote_granted: u64,
}

impl StateMachine {
    pub fn new(
        id: u64,
        member: &HashMap<u64, String>,
        election_timeout_range: Range<u64>,
    ) -> (Self, mpsc::Receiver<Operation>) {
        let mut member_info = HashMap::new();
        for (member_id, addr) in member {
            let (tx, rx) = mpsc::channel(CHAN_BUF_TODO);
            member_info.insert(
                *member_id,
                MemberInfo {
                    _addr: addr.clone(),
                    tx,
                    next_index: 0,
                    match_index: 0,
                },
            );
            tokio::spawn(grpc::connect(addr.clone(), rx));
        }

        let (committed_tx, committed_rx) = mpsc::channel(CHAN_BUF_TODO);
        let (operation_tx, operation_rx) = mpsc::channel(CHAN_BUF_TODO);
        let (applied_tx, applied_rx) = mpsc::channel(CHAN_BUF_TODO);
        let apply_status = Arc::new(Mutex::new(ApplyStatus {
            last_applied: 0,
            waiter: HashMap::new(),
        }));
        tokio::spawn(consume_applied_rx(applied_rx, apply_status.clone()));
        tokio::spawn(consume_committed_rx(committed_rx, operation_tx, applied_tx));

        (
            Self {
                id,
                election_timeout_range,
                log: Log::default(),
                commit_index: 0,
                committed_tx,
                apply_status,
                state: State::default(),
                member_info,
                election_elapsed: 0,
                election_timeout: 0,
                current_term: 0,
                voted_for: 0,
                vote_granted: 0,
            },
            operation_rx,
        )
    }

    fn quorum(&self) -> u64 {
        self.member_info.len() as u64 / 2 + 1
    }

    fn reset_election_timeout(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = if let State::Leader = self.state {
            1
        } else {
            rand::rng().random_range(self.election_timeout_range.clone())
        };
    }

    async fn update_commit_index(&mut self) {
        let mut match_index_vec: Vec<u64> =
            self.member_info.values().map(|v| v.match_index).collect();
        match_index_vec.sort();
        let commit_index = match_index_vec[self.quorum() as usize - 1];
        let log = self.log.get(&commit_index).unwrap();
        if log.term == self.current_term {
            let prev_commit_index = self.commit_index;
            self.commit_index = commit_index;
            for index in prev_commit_index + 1..=self.commit_index {
                self.committed_tx
                    .send(self.log.get(&index).unwrap().data)
                    .await
                    .unwrap();
            }
        }
    }

    async fn broadcast(&mut self, content: Content) {
        let message = RaftMessage {
            term: self.current_term,
            from: self.id,
            content: Some(content),
        };
        for (id, info) in &self.member_info {
            if *id == self.id {
                continue;
            }
            info.tx.send(message.clone()).await.unwrap();
        }
    }

    async fn send(&mut self, to: &u64, content: Content) {
        let Some(info) = self.member_info.get(to) else {
            tracing::error!("send to unknown id({})", to);
            return;
        };
        info.tx
            .send(RaftMessage {
                term: self.current_term,
                from: self.id,
                content: Some(content),
            })
            .await
            .unwrap();
    }

    async fn send_append_entries(&mut self) {
        for (id, info) in &self.member_info {
            if *id == self.id {
                continue;
            }

            let prev_log_index = info.next_index - 1;
            let prev_log_term = if prev_log_index == 0 {
                0
            } else {
                self.log.get(&prev_log_index).unwrap().term
            };
            let entries = self.log.get(&info.next_index..).unwrap();

            info.tx
                .send(RaftMessage {
                    term: self.current_term,
                    from: self.id,
                    content: Some(Content::RequestAppendEntries(RequestAppendEntries {
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit: self.commit_index,
                    })),
                })
                .await
                .unwrap();
        }
    }

    async fn transition(&mut self, term: u64, state: State) {
        tracing::info!(term, "transition to {}", state);
        self.current_term = term;
        self.state = state;
        self.voted_for = 0;
        self.reset_election_timeout();

        match self.state {
            State::Follower => {}
            State::Candidate => {
                self.voted_for = self.id;
                self.vote_granted = 1;
                let (last_log_index, last_log_term) = self.log.last_entry_info();
                self.broadcast(Content::RequestVote(RequestVote {
                    last_log_index,
                    last_log_term,
                }))
                .await;
            }
            State::Leader => {
                let (last_log_index, _) = self.log.last_entry_info();
                for (member_id, info) in self.member_info.iter_mut() {
                    info.next_index = last_log_index + 1;
                    info.match_index = if *member_id == self.id {
                        last_log_index
                    } else {
                        0
                    };
                }
                self.send_append_entries().await;
            }
        }
    }

    async fn tick(&mut self) {
        self.election_elapsed += 1;
        if self.election_elapsed >= self.election_timeout {
            match self.state {
                State::Follower | State::Candidate => {
                    self.transition(self.current_term + 1, State::Candidate)
                        .await;
                }
                State::Leader => {
                    self.send_append_entries().await;
                }
            }
        }
    }

    async fn step(&mut self, message: RaftMessage) {
        if message.term < self.current_term {
            tracing::warn!(
                term = self.current_term,
                from = message.from,
                "message from old term({}): {:?}",
                message.term,
                message.content,
            );
            return;
        }
        if message.term > self.current_term {
            self.transition(message.term, State::Follower).await;
        }
        let Some(content) = message.content else {
            tracing::error!(
                term = self.current_term,
                from = message.from,
                "empty message",
            );
            return;
        };

        match content {
            Content::RequestVote(request) => {
                let vote_granted = if self.voted_for != 0 && self.voted_for != message.from {
                    false
                } else {
                    let (last_log_index, last_log_term) = self.log.last_entry_info();
                    (request.last_log_term > last_log_term)
                        || (request.last_log_term == last_log_term
                            && request.last_log_index >= last_log_index)
                };
                if vote_granted {
                    self.reset_election_timeout();
                }
                self.send(
                    &message.from,
                    Content::ResponseVote(ResponseVote { vote_granted }),
                )
                .await;
            }
            Content::ResponseVote(resposne) => {
                if resposne.vote_granted {
                    self.vote_granted += 1;
                    if let State::Candidate = self.state {
                        if self.vote_granted >= self.quorum() {
                            self.transition(self.current_term, State::Leader).await;
                        }
                    }
                }
            }
            Content::RequestAppendEntries(request) => {
                if let State::Candidate = self.state {
                    self.transition(self.current_term, State::Follower).await;
                } else {
                    self.reset_election_timeout();
                }

                let success = self.log.append(
                    &request.prev_log_index,
                    &request.prev_log_term,
                    request.entries,
                );
                let (last_log_index, _) = self.log.last_entry_info();
                let prev_commit_index = self.commit_index;
                self.commit_index = cmp::min(request.leader_commit, last_log_index);
                for index in prev_commit_index + 1..=self.commit_index {
                    self.committed_tx
                        .send(self.log.get(&index).unwrap().data)
                        .await
                        .unwrap();
                }

                self.send(
                    &message.from,
                    Content::ResponseAppendEntries(ResponseAppendEntries {
                        success,
                        last_log_index,
                    }),
                )
                .await;
            }
            Content::ResponseAppendEntries(response) => {
                let Some(info) = self.member_info.get_mut(&message.from) else {
                    tracing::error!(
                        "ignore ResponseAppendEntries form unknown id({})",
                        message.from
                    );
                    return;
                };
                if response.success {
                    info.match_index = response.last_log_index;
                    info.next_index = info.match_index + 1;

                    if info.match_index > self.commit_index {
                        self.update_commit_index().await;
                    }
                } else {
                    info.next_index -= 1;
                }
            }
        }
    }

    async fn propose(&mut self, data: Vec<u8>, tx: oneshot::Sender<Result<Vec<u8>, RaftError>>) {
        if let State::Leader = self.state {
        } else {
            let _ = tx.send(Err(RaftError::NotLeader));
            return;
        }

        self.log.push(self.current_term, data);
        let (last_log_index, _) = self.log.last_entry_info();
        let info = self.member_info.get_mut(&self.id).unwrap();
        info.match_index = last_log_index;
        info.next_index = last_log_index + 1;

        let mut apply_status = self.apply_status.lock().await;
        apply_status.waiter.insert(last_log_index, tx);
        drop(apply_status);

        if self.member_info.len() == 1 {
            self.update_commit_index().await;
        } else {
            self.send_append_entries().await;
        }
    }

    pub async fn serve(mut self, mut rx: mpsc::Receiver<Event>, tick_duration: time::Duration) {
        let mut timeout = tick_duration;

        loop {
            let now = time::Instant::now();
            match time::timeout(timeout, rx.recv()).await {
                Ok(Some(Event::Message(message))) => {
                    self.step(message).await;
                }
                Ok(Some(Event::Apply((data, tx)))) => {
                    self.propose(data, tx).await;
                }
                Ok(None) => {
                    break;
                }
                Err(_) => {}
            }
            let elapsed = now.elapsed();
            if elapsed > timeout {
                self.tick().await;
                timeout = tick_duration;
            } else {
                timeout -= elapsed;
            }
        }

        tracing::info!("shutdown raft state machine");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_quorum() {
        let mut member = HashMap::new();

        member.insert(1, String::from("10.0.0.1:8080"));
        let sm = StateMachine::new(1, &member, 1..2).0;
        assert_eq!(sm.quorum(), 1);

        member.insert(2, String::from("10.0.0.2:8080"));
        let sm = StateMachine::new(2, &member, 1..2).0;
        assert_eq!(sm.quorum(), 2);

        member.insert(3, String::from("10.0.0.3:8080"));
        let sm = StateMachine::new(3, &member, 1..2).0;
        assert_eq!(sm.quorum(), 2);

        member.insert(4, String::from("10.0.0.4:8080"));
        let sm = StateMachine::new(4, &member, 1..2).0;
        assert_eq!(sm.quorum(), 3);

        member.insert(5, String::from("10.0.0.5:8080"));
        let sm = StateMachine::new(5, &member, 1..2).0;
        assert_eq!(sm.quorum(), 3);

        member.insert(6, String::from("10.0.0.6:8080"));
        let sm = StateMachine::new(6, &member, 1..2).0;
        assert_eq!(sm.quorum(), 4);

        member.insert(7, String::from("10.0.0.7:8080"));
        let sm = StateMachine::new(7, &member, 1..2).0;
        assert_eq!(sm.quorum(), 4);
    }
}
