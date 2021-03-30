# Automerge persistent

This project extends [automerge-rs](https://github.com/automerge/automerge-rs)
with some persistence. There is a core trait for what functionality a persister
should have and a backend wrapper struct to utilise this.

For now, see the benches for an example of a
[sled](https://github.com/spacejam/sled) backend. Adding more backends to this
repo would be very much appreciated.

Good backends to have would be:

- filesystem
- localstorage or indexeddb in browser
- memory (for some testing I guess)
- other suggestions welcome!
