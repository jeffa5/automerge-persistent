# Automerge persistent

This project extends [automerge-rs](https://github.com/automerge/automerge-rs)
with some persistence. There is a core trait for what functionality a persister
should have and a backend wrapper struct to utilise this.

For now, see the benches for an example of a
[sled](https://github.com/spacejam/sled) backend. Adding more backends to this
repo would be very much appreciated.

Good backends to have would be:

- [x] memory (for some testing scenarios)
- [x] sled
- [x] localstorage
- [ ] indexeddb
- [x] filesystem
- other suggestions welcome!

## Usage

The `PersistentBackend` struct should be the main point of reference and should
fit in place of a normal `Backend`. It can load from the persistent storage and
automatically saves on the appropriate actions (but more efficiently).
Occasionally the user should schedule a call to `compact` if storage and load
time are of concern. This gathers the changes and saves the backend in the more
compressed form, then the old changes are removed.
