// Copyright 2020-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Subscriptions that deliver into a channel the caller owns.

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::{Command, Message};

/// Where a subscription's messages go: its own channel, or a caller's channel tagged by sid.
#[derive(Debug)]
pub(crate) enum Delivery {
    Own(mpsc::Sender<Message>),
    Shared(mpsc::Sender<(u64, Message)>),
}

impl Delivery {
    pub(crate) fn try_send(&self, sid: u64, message: Message) -> Result<(), TrySendError<()>> {
        match self {
            Delivery::Own(sender) => sender.try_send(message).map_err(forget),
            Delivery::Shared(sender) => sender.try_send((sid, message)).map_err(forget),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        match self {
            Delivery::Own(sender) => sender.is_closed(),
            Delivery::Shared(sender) => sender.is_closed(),
        }
    }
}

fn forget<T>(error: TrySendError<T>) -> TrySendError<()> {
    match error {
        TrySendError::Full(_) => TrySendError::Full(()),
        TrySendError::Closed(_) => TrySendError::Closed(()),
    }
}

/// A subscription made with [crate::Client::subscribe_with], which unsubscribes on drop.
#[derive(Debug)]
pub struct Route {
    sid: u64,
    sender: mpsc::Sender<Command>,
}

impl Route {
    pub(crate) fn new(sid: u64, sender: mpsc::Sender<Command>) -> Route {
        Route { sid, sender }
    }

    /// The subscription id that tags this route's messages.
    pub fn sid(&self) -> u64 {
        self.sid
    }
}

impl Drop for Route {
    fn drop(&mut self) {
        let sid = self.sid;
        let unsubscribe = move || Command::Unsubscribe { sid, max: None };
        if let Err(TrySendError::Full(_)) = self.sender.try_send(unsubscribe()) {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let sender = self.sender.clone();
                runtime.spawn(async move {
                    sender.reserve().await.map(|slot| slot.send(unsubscribe()))
                });
            }
        }
    }
}
