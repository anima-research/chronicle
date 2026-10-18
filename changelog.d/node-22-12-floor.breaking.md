- **Node.js before 22.12:** `@animalabs/chronicle` now declares `engines.node`
  `>=22.12.0`, and CI tests Node 22.12.0, 24 and the latest Node. Node 20
  reached its end of life on April 30. **Who needs to act:** anyone installing
  on an older Node, including Node 22 before 22.12. npm warns there, and
  refuses with `engine-strict`; Yarn 1 refuses unless told to ignore engines.
  **Migration:** move to Node 22.12 or later. **Unchanged:** the native
  binding, its API and the on-disk format; existing stores keep opening.
