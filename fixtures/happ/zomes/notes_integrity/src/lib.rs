use hdi::prelude::*;

#[hdk_entry_helper]
#[derive(Clone)]
pub struct Note {
    pub text: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[hdk_entry_types]
#[unit_enum(UnitEntryTypes)]
pub enum EntryTypes {
    Note(Note),
}

/// Integrity rules are intentionally trivial: this hApp exists to exercise
/// the relay, and Holochain's system validation still runs on every op.
#[hdk_extern]
pub fn validate(_op: Op) -> ExternResult<ValidateCallbackResult> {
    Ok(ValidateCallbackResult::Valid)
}
