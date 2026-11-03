use crate::error::Result;
use crate::records::RecordLog;
use crate::types::{Sequence, StateOperation, StateUpdateRecord};

pub(crate) struct ChainWalk<'a> {
    log: &'a RecordLog,
    next: Option<u64>,
    at: Option<Sequence>,
    first: Option<u64>,
    behind_delta: bool,
    done: bool,
}

impl<'a> ChainWalk<'a> {
    pub(crate) fn new(log: &'a RecordLog, head_offset: u64, at: Option<Sequence>) -> Self {
        Self {
            log,
            next: Some(head_offset),
            at,
            first: None,
            behind_delta: false,
            done: false,
        }
    }

    pub(crate) fn first_offset(&self) -> Option<u64> {
        self.first
    }

    fn read(&mut self) -> Option<Result<StateOperation>> {
        while let Some(offset) = self.next {
            let update = match self
                .log
                .read_at(offset)
                .and_then(|record| Ok((record.sequence, StateUpdateRecord::decode(&record)?)))
            {
                Ok(read) => read,
                Err(e) => return Some(Err(e)),
            };
            let (sequence, update) = update;
            self.next = update.prev_update_offset;
            if self.at.is_some_and(|at| sequence > at) {
                continue;
            }
            self.first.get_or_insert(offset);
            return Some(Ok(update.operation));
        }
        None
    }
}

impl Iterator for ChainWalk<'_> {
    type Item = Result<StateOperation>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            let op = match self.read() {
                None => {
                    self.done = true;
                    return None;
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Some(Err(e));
                }
                Some(Ok(op)) => op,
            };
            match op {
                StateOperation::Snapshot(_) | StateOperation::Set(_) => {
                    self.done = true;
                    return Some(Ok(op));
                }
                StateOperation::DeltaSnapshot(_) | StateOperation::TreeDeltaSnapshot(_) => {
                    self.behind_delta = true;
                    return Some(Ok(op));
                }
                _ if self.behind_delta => {}
                _ => return Some(Ok(op)),
            }
        }
    }
}
