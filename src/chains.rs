use std::collections::{BTreeMap, HashMap};

use parking_lot::RwLock;

use crate::chain::Chain;

pub struct Chains {
    data: RwLock<HashMap<String, &'static RwLock<Chain>>>,
    aliases: BTreeMap<String, String>,
}

impl Chains {
    pub fn new(aliases: BTreeMap<String, String>) -> Self {
        Self {
            data: Default::default(),
            aliases,
        }
    }

    pub fn chain(&self, name: &str) -> &'static RwLock<Chain> {
        let name = self.aliases.get(name).map(|a| a.as_str()).unwrap_or(name);
        {
            let reader = self.data.read();
            if let Some(chain) = reader.get(name) {
                return chain;
            }
        }
        {
            let mut writer = self.data.write();
            if !writer.contains_key(name) {
                writer.insert(name.to_string(), Box::leak(Default::default()));
            }
        }
        self.data.read().get(name).unwrap()
    }
}
