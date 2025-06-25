use std::ops::RangeFrom;

use super::pb::LogEntry;

pub trait LogOffset {
    type Output;
    fn get_from(self, entries: &[LogEntry], snapshot_index: &u64) -> Self::Output;
}

impl LogOffset for &u64 {
    type Output = Option<LogEntry>;
    fn get_from(self, entries: &[LogEntry], snapshot_index: &u64) -> Self::Output {
        if self <= snapshot_index {
            None
        } else {
            let index = self - snapshot_index - 1;
            entries.get(index as usize).cloned()
        }
    }
}

impl LogOffset for RangeFrom<&u64> {
    type Output = Option<Vec<LogEntry>>;
    fn get_from(self, entries: &[LogEntry], snapshot_index: &u64) -> Self::Output {
        if self.start <= snapshot_index {
            None
        } else {
            let start = self.start - snapshot_index - 1;
            entries.get(start as usize..).map(|v| v.to_vec())
        }
    }
}

#[derive(Default)]
#[cfg_attr(test, derive(Debug, PartialEq))]
struct Snapshot {
    index: u64,
    term: u64,
    _data: Vec<u64>,
}

#[derive(Default)]
#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct Log {
    snapshot: Snapshot,
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn get<I>(&self, index: I) -> I::Output
    where
        I: LogOffset,
    {
        index.get_from(&self.entries, &self.snapshot.index)
    }

    pub fn push(&mut self, term: u64, data: Vec<u8>) {
        self.entries.push(LogEntry { term, data });
    }

    pub fn append(
        &mut self,
        prev_entry_index: &u64,
        prev_entry_term: &u64,
        mut entries: Vec<LogEntry>,
    ) -> bool {
        if let Some(entry) = self.get(prev_entry_index) {
            if *prev_entry_term != entry.term {
                return false;
            }
        } else if (*prev_entry_index != 0 || *prev_entry_term != 0)
            && (*prev_entry_index != self.snapshot.index || *prev_entry_term != self.snapshot.term)
        {
            return false;
        }

        self.entries
            .truncate((prev_entry_index - self.snapshot.index) as usize);
        self.entries.append(&mut entries);
        true
    }

    pub fn last_entry_info(&self) -> (u64, u64) {
        let l = self.entries.len();
        if l == 0 {
            (self.snapshot.index, self.snapshot.term)
        } else {
            let entry = self.entries.last().unwrap();
            (self.snapshot.index + l as u64, entry.term)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get() {
        let mut l = Log::default();
        l.push(1, Vec::from([1]));
        l.push(1, Vec::from([2]));
        l.push(1, Vec::from([3]));
        assert_eq!(
            l.get(&2),
            Some(LogEntry {
                term: 1,
                data: Vec::from([2])
            })
        );
        assert_eq!(
            l.get(&2..),
            Some(Vec::from([
                LogEntry {
                    term: 1,
                    data: Vec::from([2])
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([3])
                }
            ]))
        );
        assert_eq!(l.get(&4..), Some(Vec::from([])));
        assert_eq!(l.get(&5..), None);

        let mut l = Log::default();
        l.snapshot.index = 10;
        l.snapshot.term = 3;
        l.push(3, Vec::from([11]));
        l.push(3, Vec::from([12]));
        l.push(3, Vec::from([13]));
        assert_eq!(l.get(&2), None);
        assert_eq!(
            l.get(&12),
            Some(LogEntry {
                term: 3,
                data: Vec::from([12])
            })
        );
        assert_eq!(l.get(&2..), None);
        assert_eq!(
            l.get(&12..),
            Some(Vec::from([
                LogEntry {
                    term: 3,
                    data: Vec::from([12])
                },
                LogEntry {
                    term: 3,
                    data: Vec::from([13])
                }
            ]))
        );
        assert_eq!(l.get(&14..), Some(Vec::from([])));
        assert_eq!(l.get(&15..), None);
    }

    #[test]
    fn test_append() {
        let mut l = Log::default();
        assert_eq!(l.append(&1, &1, Vec::from([])), false);
        assert_eq!(
            l.append(
                &0,
                &0,
                Vec::from([
                    LogEntry {
                        term: 1,
                        data: Vec::from([1]),
                    },
                    LogEntry {
                        term: 1,
                        data: Vec::from([2]),
                    },
                ]),
            ),
            true
        );
        assert_eq!(
            l.entries,
            Vec::from([
                LogEntry {
                    term: 1,
                    data: Vec::from([1]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([2]),
                },
            ])
        );

        assert_eq!(l.append(&2, &2, Vec::from([])), false);
        assert_eq!(l.append(&3, &1, Vec::from([])), false);
        assert_eq!(
            l.append(
                &2,
                &1,
                Vec::from([
                    LogEntry {
                        term: 1,
                        data: Vec::from([3]),
                    },
                    LogEntry {
                        term: 1,
                        data: Vec::from([4]),
                    },
                ]),
            ),
            true
        );
        assert_eq!(
            l.entries,
            Vec::from([
                LogEntry {
                    term: 1,
                    data: Vec::from([1]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([2]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([3]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([4]),
                },
            ])
        );

        assert_eq!(
            l.append(
                &2,
                &1,
                Vec::from([
                    LogEntry {
                        term: 1,
                        data: Vec::from([5]),
                    },
                    LogEntry {
                        term: 1,
                        data: Vec::from([6]),
                    },
                ]),
            ),
            true
        );
        assert_eq!(
            l.entries,
            Vec::from([
                LogEntry {
                    term: 1,
                    data: Vec::from([1]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([2]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([5]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([6]),
                },
            ])
        );

        let mut l = Log::default();
        l.snapshot.index = 5;
        l.snapshot.term = 1;
        assert_eq!(l.append(&4, &1, Vec::from([])), false);
        assert_eq!(l.append(&6, &1, Vec::from([])), false);
        assert_eq!(
            l.append(
                &5,
                &1,
                Vec::from([
                    LogEntry {
                        term: 1,
                        data: Vec::from([6]),
                    },
                    LogEntry {
                        term: 1,
                        data: Vec::from([7]),
                    },
                ]),
            ),
            true
        );
        assert_eq!(
            l.entries,
            Vec::from([
                LogEntry {
                    term: 1,
                    data: Vec::from([6]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([7]),
                },
            ])
        );

        assert_eq!(
            l.append(
                &7,
                &1,
                Vec::from([
                    LogEntry {
                        term: 1,
                        data: Vec::from([8]),
                    },
                    LogEntry {
                        term: 1,
                        data: Vec::from([9]),
                    },
                ]),
            ),
            true
        );
        assert_eq!(
            l.entries,
            Vec::from([
                LogEntry {
                    term: 1,
                    data: Vec::from([6]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([7]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([8]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([9]),
                },
            ])
        );

        assert_eq!(
            l.append(
                &7,
                &1,
                Vec::from([
                    LogEntry {
                        term: 1,
                        data: Vec::from([10]),
                    },
                    LogEntry {
                        term: 1,
                        data: Vec::from([11]),
                    },
                ]),
            ),
            true
        );
        assert_eq!(
            l.entries,
            Vec::from([
                LogEntry {
                    term: 1,
                    data: Vec::from([6]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([7]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([10]),
                },
                LogEntry {
                    term: 1,
                    data: Vec::from([11]),
                },
            ])
        );
    }

    #[test]
    fn test_last_entry_info() {
        let mut l = Log::default();
        assert_eq!(l.last_entry_info(), (0, 0));
        l.push(1, Vec::new());
        assert_eq!(l.last_entry_info(), (1, 1));
        l.push(1, Vec::new());
        assert_eq!(l.last_entry_info(), (2, 1));
        l.push(2, Vec::new());
        assert_eq!(l.last_entry_info(), (3, 2));

        let mut l = Log::default();
        l.snapshot.index = 10;
        l.snapshot.term = 3;
        assert_eq!(l.last_entry_info(), (10, 3));
        l.push(3, Vec::new());
        assert_eq!(l.last_entry_info(), (11, 3));
        l.push(4, Vec::new());
        assert_eq!(l.last_entry_info(), (12, 4));
    }
}
