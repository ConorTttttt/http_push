use hdk::prelude::*;
use notes_integrity::*;

/// Signal emitted after a note is committed. The relay forwards only a
/// digest of it, never the payload.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NotesSignal {
    NoteCreated { action_hash: ActionHash },
}

#[hdk_extern]
pub fn create_note(text: String) -> ExternResult<ActionHash> {
    create_entry(EntryTypes::Note(Note { text }))
}

#[hdk_extern]
pub fn get_note(hash: ActionHash) -> ExternResult<Option<Record>> {
    get(hash, GetOptions::default())
}

#[hdk_extern(infallible)]
pub fn post_commit(committed: Vec<SignedActionHashed>) {
    for a in committed {
        if a.action().action_type() == ActionType::Create {
            let _ = emit_signal(NotesSignal::NoteCreated {
                action_hash: a.as_hash().clone(),
            });
        }
    }
}
