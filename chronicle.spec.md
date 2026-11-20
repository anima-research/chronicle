# chronicle: the package's spec

This file says what the package does for a caller, and how a caller reaches it.

- **A functional item (F)** says what the package does, in words that hold whatever its code is.
- **The interface items** say how a caller reaches these functions. Each is one of six kinds: a rule of the whole Node interface (N), a method (A), a type (T), the failures (E), a file of the store (D), or the Rust crate (R).
- **Each item has a label.** The name of a test holds the label of the item under test.
- **A change to the package that changes an item changes this file in the same pull request.**

## Objective

chronicle stores an application's records and named states in one directory. The application can read each state as it was at any earlier point, and it can keep branches that change apart from each other.

- The store is a log of records. A record never changes after it is appended, and each change of a state is one of those records.
- A caller can read a state as it was at any earlier sequence of its branch.
- A branch starts at a sequence of another branch. It starts with the states of that branch, as they were at that sequence. After that, each branch changes alone.
- A blob is bytes stored under their hash. A read gives the same bytes back, or it fails.
- A subscriber names the changes that it wants and a sequence to start from. It gets the changes since that sequence, and then each new change as an event.

## The package

The package is the Rust crate `chronicle` with its Node binding. Together they are the npm package `@animalabs/chronicle`. The repository also holds `server/`, `ui/` and `tools/`, which are no part of the package.

## Terms

- **Store.** One directory that holds an application's records, states, branches and blobs.
- **Caller.** The application that has a store open.
- **Record.** One entry of the store's log. It has an ID, a branch, a sequence, a type, a payload, a time, and two lists of links to other records.
- **Sequence.** A whole number that says where a record is on its branch.
- **State.** A named value of a store. Each branch has its own value of each state. A value is a whole value, a list of items, or a tree. A caller registers a state with one of these three kinds. The package's own words for them are `snapshot`, `append_log` and `tree`.
- **Branch.** A named line of records. Each branch but `main` has a parent and a branch point. The parent is another branch. The branch point is a sequence of the parent.
- **Head.** Every branch has a head. It is the sequence of the newest record appended on the branch. Without a record of its own, a branch has its branch point as head.
- **Current branch.** The store acts on this branch when the caller names no branch. A store has one such branch.
- **View.** A caller's way to act on one named branch, whatever the current branch is.
- **The caller's branch.** The branch that a caller's request acts on. For a request through a view, it is the view's branch. For any other request, it is the current branch.
- **Refused.** The store does not do what was asked, changes nothing, and tells the caller why.
- **Fails.** The store reports an error. A request that fails can leave a change behind, and the item says which.
- **An answer that depends on a branch.** The records of a branch, its head, and each read of a state. An answer can cover every branch. Such an answer does not depend on a branch. These are such answers: a read of a record by its ID, the records of a type, and the answers about links.

## Functional items

### The store

**F1. A new store.** A caller creates a store in a directory that is empty or does not exist yet. The directory then holds a store with one branch, named `main`. That branch has no parent, its head is zero, and it is the current branch. The store has no records, no states and no blobs.

**F2. No store is created over a store.** If the directory already holds a store, the create is refused. A store has four data files. If the directory holds one of them but no whole store, the create is refused too.

**F3. Open.** When a caller opens a directory that holds a store, the caller gets that store. When the directory holds no store, the open is refused.

**F4. Open or create.** A caller can ask to open or create a store. If the directory holds a store, the caller gets that store. When the directory is empty or does not exist yet, it gets a new store, as in F1. If the directory holds no store but holds other files, the request is refused. A store is not put into such a directory. Some names do not count as other files. The list is fixed and has two kinds of name. Common operating systems put entries of their own into an empty volume or folder. These entries are the first kind. The second kind is what an unfinished create leaves.

**F5. One process at a time.** While one process has a store open, another process cannot open or create that store. Its request is refused. The refusal says that the store is locked.

**F6. Close.** When a caller closes a store, the store saves everything accepted so far. Another process can then open the store. After the close, the store refuses each request to read or change its records, states, branches or blobs. The refusal says that the store is closed. Limit today: L4.

### Records

**F7. Append.** When a caller appends a record, it gives a type and a payload, and it can give links. The store adds one record to the caller's branch. The record's sequence is one more than the branch's head. So the record becomes the branch's new head. The store gives the record an ID and the time of the append. The answer is the record.

**F8. A record's ID.** No two records of a store have the same ID. Each new record gets a greater ID than every record accepted before it. This holds across all branches. The part on crashes has one exception: when a crash loses the newest records, later records get their IDs.

**F9. A record does not change.** After the store accepts a record, the record keeps its ID, its branch, its sequence, its type, its payload, its time and its links. Each read of the record gives the same values. This still holds after the record's branch is deleted.

**F10. A reserved type.** The record type `state_update` belongs to the changes of states. An append of a record with that type is refused.

**F11. Read by ID.** When a caller reads a record by its ID, the store gives that record, whatever branch it is on. When no record has the ID, the answer says so. When a caller reads several IDs in one request, the answer has one entry for each ID, in the order of the request. An ID can be in the request more than one time.

**F12. Links.** A record names other records in two lists. The first list holds the records that caused it. The second list holds records that are related to it in another way. A caller can name a record R and ask what it caused. The answer holds the IDs of the records that name R in their first list. A caller can ask what is related to R. The answer holds the IDs of the records that name R in their second list. Both answers hold records of every branch. Limit today: L2.

**F13. The records of a branch.** A caller can ask for records in a range of sequences. The answer holds each record that was appended on the caller's branch in that range. The first and the last sequence of the range are in the range. When the first sequence is after the last sequence, the range holds no records. The answer holds no record of the branch's parent. This is so also for a record of the parent from before the branch point. Limits today: L1 and L10.

**F14. The order and the limits of that answer.** The answer is in ascending order of sequence. When the caller asks for the newest first, it is in descending order. When the caller names types, the answer holds only records of those types. When the caller gives a limit, the answer holds at most that many records. They are the first matching records in the answer's order. With no limit given, the limit is 10,000. The caller can give a number of records to skip. The answer then starts after that many matching records. Limit today: L1.

**F15. The records of a type.** A caller can ask for the records of a type. The store gives the ID of each record of that type, from every branch, in ascending order of ID.

**F16. The head.** A caller can ask for the head. The answer is the head of the caller's branch.

### Branches

**F17. A new branch at the head.** A caller creates a branch with a new name from a branch P. The store then adds a branch with that name. Its parent is P. Its branch point is P's head at that moment, and its head is the same sequence. When the caller names no P, P is the caller's branch. Limit today: L3.

**F18. A new branch at an earlier sequence.** A caller can create a branch from P at a sequence of its choice. That sequence can be at most P's head. It is then the branch point of the new branch, and its head too. A sequence greater than P's head is refused.

**F19. Names.** No two branches of a store have the same name at the same time. A create is refused when a branch already has the name. If the branch named as P does not exist, the create is refused. After a branch is deleted, a new branch can take its name.

**F20. What a new branch starts with.** On the new branch, every state begins as P had it at the branch point. The caller can choose which states the new branch takes from P, by their names or by the start of their names. It can also choose none. A state that is not taken has no value on the new branch. The new branch has no records of its own.

**F21. Branches change apart.** An append on one branch changes no answer that depends on another branch. A change of a state on one branch changes no answer that depends on another branch. Both hold between a branch and its parent, in both directions, from the moment when the branch is created.

**F22. Sequences after the branch point.** The first record appended on a new branch gets a sequence one above the branch point. So two branches can each hold a record with the same sequence. These are two records, with two IDs.

**F23. The current branch.** A store has one current branch at each moment. A caller can make another branch the current branch. After that, each request without a branch name acts on the new current branch. The store keeps the current branch across a close and an open. A request to make a branch current is refused when that branch does not exist.

**F24. A view.** A caller can take a view of a branch by its name. Requests through a view act on the view's branch, whenever a branch matters. A change of the current branch does not change this. A request through a view to change the current branch is refused. After the view's branch is deleted, the view refuses each request that depends on a branch. The refusal says that the branch is not found. Limit today: L4. A caller can close a view, and the store and its other views then go on as before. When the store is closed, each of its views is closed too.

**F25. Delete.** When a caller deletes a branch, the branch is in no list of branches. A request to act on it is refused, and the refusal says that the branch is not found. Its parent and its sibling branches read as before. If a branch's parent is deleted, the branch also reads as before. It keeps the states that it took from the parent, with their history. The records that were appended on the deleted branch stay in the store, and a caller can read each by its ID. A delete of the branch `main` is refused. A delete of the current branch is refused. A view cannot delete its own branch: that delete is refused.

**F26. The list of branches.** When a caller asks for the branches, the store gives each branch with its identity, its name, its head, its parent, its branch point and the time when it was created. A branch names its parent by identity. If the parent is deleted, the branch still names it. It keeps naming the deleted parent, even after a new branch takes that parent's name.

**F80. A branch's identity.** Each branch has an identity. No other branch of the store has the same one. Creating a branch under a deleted branch's name gives a new identity. The new branch does not get the deleted branch's identity. A caller can read a branch's identity, and it does not change while the branch exists.

### States

**F27. Register.** When a caller registers a state, it gives a name and a kind. The kind is a whole value, a list, or a tree. The registration holds for every branch of the store, and it gives the state no value. A register is refused when its name is already registered. The refusal says that the state exists. The store keeps a registration across a close and an open.

**F77. A value without a registration.** A caller can set and read a state that is not registered. Such a state is not in the list of states. When the caller registers the name afterwards, the state keeps its value.

**F28. The list of states.** When a caller asks for the states, the store gives each registered state with its name and its kind. For a list, it also gives the number of items on the caller's branch, when the list has a value there. Limit today: L11.

**F29. Each change is a record.** Each change of a state adds one record of the reserved type to the caller's branch. The record takes the branch's next sequence, as in F7. The answer to the change is that record. A change of a state on one branch gives the state a value on that branch alone. The store can add records of its own of the reserved type to the branch, so that reads stay fast. Each of them takes a sequence too. So after one change, the branch's head can be more than one above where it was.

**F78. What a change of a state was.** A record can be a change of a state. A caller can give the ID of such a record. The store then tells which state changed and what the change was. The store can add a record of its own for a state. For such a record, the store answers in the same way. The ID can be of a record of another type, or of no record. The answer then says that there is no such change.

**F30. A whole value.** A whole value is bytes. A caller can also set it and read it as a JSON value. A caller sets a state of the kind whole value. Each read of the state on that branch then gives the set value. This holds until the next change of the state on that branch. When a state has no value on the branch, a read says so. If the value is the JSON null, a read as JSON gives that same answer. A read as bytes tells the two apart. If the bytes are no JSON, a read as JSON fails. The bytes still read as bytes.

**F31. A list: append.** An item of a list is a JSON value. A caller can also give an item as bytes, and the store does not check that those bytes are JSON. When a caller appends an item to a list, the item goes to the end of the list. Its index is the number of items that the list had before, so the first item has the index zero.

**F32. A list: several items in one change.** A caller can append several items in one change. The store adds them to the end of the list in the order given. The change is one record with one sequence, and its answer gives the index of the first item. For a read, it makes no difference whether the items were appended together or one at a time. A change with no items is refused.

**F33. A change of several items is whole.** A read at an earlier sequence holds every item of such a change or none of them. A branch created at a sequence holds all items of such a change, or none. After a crash, the list holds every item of such a change or none of them.

**F34. An item that carries its own place.** A caller can ask the store to write two values into the item: the ID of the change's record, and that record's sequence. The caller names the two keys. When the change has several items, each item holds the ID and the sequence of that one record. The request is refused when an item is not a JSON object. A change of several items is also refused when the two keys are the same. A change of one item takes two equal keys, and the item then holds the sequence at that key.

**F35. A list: edit.** When a caller edits the item at an index, that item has the new value, and each other item keeps its index and its value. If the list has no item at the index, the edit is refused.

**F36. A list: redact.** When a caller redacts a range of indexes, the list loses the items in the range. The range starts at its first index and stops before its end: the item at the end index stays. Each later item moves down by the number of items that left. A part of the range that is beyond the end of the list is ignored.

**F79. Replace a whole list.** A caller can replace the whole value of a list with a new list in one change. A read at an earlier sequence gives the old items, and a read at the change's sequence gives the new items. Each read gives the old list or the new list, and never a part of one. A later append goes to the end of the new list.

**F37. Reads of a list.** A caller can read a list in six ways: the whole list, the number of its items, the items in a range of indexes, the last items up to a number that it gives, one item by its index, and several items by their indexes in one request. An index that the list does not have gives "no item". The items of one request are all from one version of the list. Limit today: L11.

**F38. A tree.** A value of the kind tree is a map from paths to entries. An entry names a blob by its hash, and has a size and a mode. A caller can set the entry of a path and remove a path. A caller can make several such sets and removes as one change. The tree gets all of them or none, and the change is one record. A caller can read the entry of one path. It can list all the paths, or only the paths that start with a given text.

**F39. Two versions of a tree.** When a caller asks what changed in a tree between two sequences, the store gives each path that was added, each path that was removed, and each path whose entry changed, with the old and the new entry.

**F40. A read at an earlier sequence.** A caller can read a state at a sequence S. The answer is the state's value at S on the caller's branch. It is the value after all changes with a sequence of at most S. Changes from the branch's parent count too, up to the branch point. When the state had no value at S, the answer says that it has no value.

**F41. A number that follows a state's value.** If a state has a value on a branch, the store gives a number for it. The number changes at each change of that state on that branch. It does not change at a change of another state, and it does not change at a change on another branch. Each view of the branch gets the same number. A caller can keep a copy of the value. The number tells it when to read again. A new number tells the caller to read again. It does not promise that the value differs.

**F42. An index on a field of a list's items.** A caller can register an index for its branch. It indexes one field of a list's items. The index takes the field as numbers or as texts. The store takes each number as a floating-point number of 64 bits. The caller can then ask which items have the field in a range of numbers. The answer holds the indexes of those items, in ascending or descending order of the field, and the caller can give a limit and a number to skip. The caller can ask which items have the field equal to a given text, and the answer holds the indexes of those items. It can ask for each text of the field, with the count of items for that text. The index follows each later change of the list on that branch.

**F43. An index belongs to one branch.** On a branch where the caller has not registered the index, the answer is "no index". That answer differs from "no item matches". A change of the list on another branch does not change a branch's index and does not remove it. After a crash, an index can be gone. The answer is then "no index" until the caller registers it again.

**F44. A change with a JSON value leaves the state readable.** A caller changes a readable state and gives each value as a JSON value. After such a change, each read of the state as JSON on that branch gives its value. This holds for a change that fits the value. A set fits every value. An append, an edit and a redact fit a JSON array. The store accepts an append to a value that is no JSON array, and the state then does not read.

**F45. A change of a path needs a tree.** If the state is not registered as a tree, the store refuses a set or a remove of a path.

### Blobs

**F46. Store a blob.** When a caller stores bytes with a content type, the store keeps the bytes under their SHA-256 hash, and the answer is that hash. The same bytes always get the same hash, so the store keeps the same bytes one time. A content type of more than 65,535 bytes is refused.

**F47. Read a blob.** When a caller reads a blob by its hash, the store gives the bytes that were stored under that hash. When the store holds no blob with that hash, the answer says so. When the store reads a kept copy that is damaged or cut short, the read fails. After the first read of a blob, the store can answer later reads from its memory, while it stays open. The store never gives bytes that differ from the bytes that were stored.

**F48. A damaged blob can be stored again.** A kept copy can be damaged or cut short. If a caller stores the same bytes again, the store replaces the copy. A later read gives the bytes. Two cases are left out. In the first case, this open store wrote or checked the copy, and the copy still has the same length. The store takes such a copy as good. That ends when a read of it fails, or when the store is opened again. The second case is a copy that names a newer format than the store knows. The store leaves that copy as it is, and the request to store the bytes fails.

**F49. A blob belongs to the store.** A blob does not belong to a branch. A caller on any branch can read it. A change of a branch leaves it as it is.

**F50. Bytes stored in the background.** A caller can ask the store to store bytes and to answer later, so that the caller can go on meanwhile. Such requests run one at a time, in the order in which they were made. A close waits for all such requests made before it.

**F51. Contents that share bytes.** A caller can ask the store to keep a content in parts. When two such contents share long runs of bytes, they also share the storage of those runs. A read gives the content as it was stored, in whichever way it was kept.

### Subscriptions

**F52. Subscribe.** A caller can subscribe to changes of the store. It says which kinds of change it follows: records, changes of states, and changes of branches. It gets a kind only when it asks for that kind. It can limit the records to some types, and the states to some names. The answer is the subscription's ID.

**F53. Events only after a catch-up.** A subscription gets no event of a change before the caller asks it to catch up. When the catch-up ends, the subscription gets an event that says so, and live events follow.

**F54. A catch-up from a sequence.** A subscription can name a sequence to start from. The catch-up then sends two things, in this order. The catch-up sends the followed states first. For each state, it sends the value at that sequence on the caller's branch. If a state had no value at that sequence, the catch-up sends its present value. After the states, it sends the followed records. These are records appended on that branch. They start at that sequence and come in ascending order of sequence. The record at that sequence itself is among them. A followed state can change after that sequence. The catch-up sends no event of that state for such a change. Such a change comes as a record of the reserved type. A subscription gets it if it follows records. Limits today: L6 and L7.

**F55. Live events.** After the catch-up, the store queues one event for each change that the subscription follows. The changes are: a record appended by a caller, a change of a state, a new head of a branch, a created branch and a deleted branch. A change of a state is one event of that state, and no event of a record. A change of several items of a list is one event. The store can add a record of its own for a state (F29). That record is an event of that state too. A caller takes the events one at a time, in the order in which the store queued them. If no event waits, the answer says so. That answer comes at once, or after a time that the caller gives.

**F56. No record is lost and none is doubled across a catch-up.** After a catch-up from a sequence S, the subscription has got each followed record with a sequence above S one time: in the catch-up, or as a live event. Limit today: L5.

**F57. A subscription and branches.** A catch-up reads the caller's branch. Live events come from every branch. A subscription can name a branch, and the store does not apply that name yet.

**F58. A large state in a catch-up.** A subscription can give a size. If a followed state is larger than that size, the catch-up sends a part of it and says so. For a JSON array, it sends the newest items. It also says where the part starts and how many items the array has. For another value, it sends no value. With no size given, the size is 10 MiB. Limit today: L8.

**F59. A subscriber that does not keep up.** A subscription's buffer holds a number of waiting events. If more events wait, the store drops the subscription. With no number given, the buffer holds 1,000 events. The caller learns of the drop later, when it asks for an event. The events queued before the drop come first. It learns of it in one of two ways. One way is an event that says so. It comes if the buffer had room for it. The other way is a refusal: it says that the subscription was dropped. When the catch-up itself overruns the buffer, the catch-up is refused in the same way.

**F60. The end of a subscription.** After a caller ends a subscription, the subscription gets no more events. A caller can ask for an event of a subscription that it does not have. That request is refused, and the refusal says that the subscription is not found. After a close of the store, a request for an event first gives each event that was queued, and is then refused with the reason that the store is closed.

### What a crash can lose

**F61. What a sync makes safe.** When a sync or a close returns, the disk holds each record that the store accepted before it. A crash of the machine after that does not lose it.

**F62. What a crash of the machine can lose.** Without a sync, a crash of the machine can lose the newest records. It loses at most the newest 100, while no flush of the log has failed. After a failed flush the store goes on to accept records, and F70 says how the caller learns of the failure. Records are lost from the end of the log alone. So after a lost record, every record accepted later is lost too.

**F63. What a crash of the process can lose.** The process can end without a close while the machine goes on. Every append that had returned keeps its record at the next open. It can lack one record at most: the one that was being written.

**F64. Changes that are safe when they return.** Four changes are on the disk when their request returns: a register of a state, a create of a branch, a delete of a branch, and a change of the current branch. This holds if the system can make a renamed file safe at once. A loss of power can undo such a change on Windows, and on any system that does not sync a directory.

**F65. Whole or not at all.** After a crash, the store holds each record whole or not at all. So the same holds for a change of several items: whole or not at all. The same goes for a change of several paths of a tree. A crash can cut off the storing of a blob. The blob then reads in one of two ways: as before the storing, or as the whole new bytes.

**F66. What the next open does.** After a crash, the next open sets each branch's head and each state's value to match the log. A state then has the value that results from the log's changes, in their order.

**F67. What the next open reports.** The caller can ask what the last open found. The answer names four things. The first is about a cut-off record at the end of the log: the number of bytes that the open removed, and the number of records left. The second is the list of states that it brought back without a registration. The third is the number of changes of states that it could not decode. The fourth is the number of changes that it skipped because their branch is not in the store. If the open found none of the four, the answer says so. If the open only brought heads and states up to the log, the answer is the same.

**F68. Damage in the middle is no crash.** A damaged record can have valid records after it in the log. The open is then refused. The store does not remove records that follow a damaged one. Limit today: L9.

**F69. IDs after a loss.** When a crash loses the newest records, later records can get the IDs of the lost records. A caller can hold such an ID from before the crash. After the crash, that ID can name another record.

**F70. A disk that fails.** When a flush of the log to the disk fails, each later sync fails, and the close fails. The error says that durability is lost. A request of F64 fails in the same way, unless the store refuses it first for a reason of its own. When a request of F64 fails in this way for the first time, its change still holds in the store's memory. Later such requests are not applied. This lasts until the store is opened again.

**F71. An older release does not misread a newer store.** A release may not know a change of several items of a list. Such a release refuses to open a store that holds such a change. A release may not know how a content is kept in parts. Such a release fails to read a content that is kept in parts, and it reads every other blob.

### Cost

**F72. A read of a state.** The time to read a state's current value depends on the size of the value. The number of earlier changes does not make that read slower. This holds while the store saves its short cuts. A caller can turn that saving off.

**F73. A read of one item.** After the store has split a list into its items in its memory, the time to read one item depends on the item's size, and not on the list's length. The store keeps a bounded number of lists in that form.

**F74. A change of a list.** The store accepts an append, an edit or a redact without a read of the list's value from the disk. At some changes the store also saves a short cut of the whole list. Such a change reads and writes the whole list.

**F75. A new branch and a view.** The log's length does not slow the creation of a branch at its parent's head. It depends on how many states the branch takes. It also depends on how much the store saves at each such change. At each such change, the store saves its table of branches, its table of states, and its indexes on fields. A branch at an earlier sequence also needs a walk back, from the parent's newest changes to that sequence. The time to take a view does not depend on the store's size.

**F76. A query of records.** A query of records takes longer when it reads more records. A larger store adds only the cost of a search in a sorted index.

## Interface items

An interface item says how a caller reaches the package's function: a name, its arguments, what it gives back, and how it fails. The items are accurate when two things hold. A package that complies with every item is a working chronicle. Today's callers can use it with no change. That holds for the Node interface. The items of the store's files and of the Rust crate are not complete in that sense yet. D3 to D6 do not state bytes. R2 lists modules and no functions. So an item states everything observable by a caller, unless the type already says it.

The signatures use the notation of the package's `index.d.ts`. The build makes that file from `src/napi.rs`. A method can fail in more ways than its rules name. N8 gives two such ways, and they hold for nearly every method.

### The Node interface, `@animalabs/chronicle`

**N1. One class.** The package gives one class, `JsStore`. A caller gets a store from one of its three static methods, and a view from `view`. A view is a `JsStore` too.

**N2. Synchronous calls.** Every method but `storeBlobAsync` runs on the caller's thread and returns its answer directly. A caller uses a store's handles only from the thread that opened the store. This is a rule for the caller. The package does not check it.

**N3. Failures.** When the store refuses or fails a call, the method throws an `Error`. Its `code` is one of the texts of E1, and its message has the form that E2 gives. Two cases are apart. An argument of a wrong type is refused before the store sees it. That error comes from the binding's runtime, and it has no code of E1. And `storeBlobAsync` reports a failure of its store by rejecting its promise. Its own machinery can fail too, such as a thread that cannot start or that panics. Such a failure has the runtime's code and no code of E1.

**N4. IDs and numbers.** A record's ID, a branch's ID and a subscription's ID are texts, and each is a whole number in decimal digits. One place is apart: the `data` of an event. There, the ID of a record and the ID of a branch are JSON numbers. An ID given to a method may have a plus sign before its digits. A sequence, an index and a count are numbers. The hash of a blob is a text of 64 hexadecimal digits in lower case. The hash in an entry of a tree is the text that the caller gave, and the store does not check it.

**N5. Names.** This list gives the name of each method and each field, in its exact spelling.

**N6. Whole numbers of zero or more.** A method takes each sequence, index, count, limit and time as a whole number of zero or more. The binding does not check this. For a number below zero, an item can say what the call does. If no item says it, the call's behaviour is not specified.

**N7. Large numbers.** A number that the package gives is exact up to 2^53. Above that, a sequence, an index, a count or a place in the log is not exact in JavaScript.

**N8. A closed handle, and a deleted branch.** On a closed handle, every method but `isClosed` and `pinnedBranch` throws `StoreClosed`. Three cases are apart. A second `close` does nothing (A4). The two polls do not look whether the handle is closed: a poll first checks the ID, and then gives the events that still wait (A66). A read of an item with an index below zero answers null (L4). A view's branch can be deleted. The view's methods then throw `BranchNotFound`, where they depend on a branch.

**N9. The order of keys.** A caller writes a JSON object. The object comes back with its keys in the order written, at every level. This holds for every method that gives it back as bytes or as a JSON text. Some methods give a JavaScript object. Its keys then come in JavaScript's own order. JavaScript puts each key that is a whole number first, in rising order. The other keys follow in the order written. The entries of a tree are apart: they come by path. If an earlier release stored the object, the object keeps its stored order.

#### Methods

**A1. `create`.** `static create(config: JsStoreConfig): JsStore`. Functional items: F1, F2, F5.
- `config.path` names a directory. `config.blobCacheSize` is the number of blobs that the open store keeps in its memory. It is 1000 when left out, and at least one.
- It returns the store's own handle, which follows the current branch.
- It throws `Locked` when another process has the store open. It looks for that first. Then it throws `InvalidOperation` when the directory holds one of a store's four data files.

**A2. `open`.** `static open(config: JsStoreConfig): JsStore`. Functional items: F3, F5, F66, F68.
- `config` is as for `create`.
- It returns the store's own handle.
- It throws `Locked` when another process has the store open, and `Corruption` when it finds a damaged record in the log with valid records after it.
- Limit today: L9.

**A3. `openOrCreate`.** `static openOrCreate(config: JsStoreConfig): JsStore`. Functional items: F4, F5.
- `config` is as for `create`.
- It opens the store when the directory holds one of the four files `MANIFEST`, `records.log`, `state.bin` and `branches.bin`. When the directory is empty or does not exist, it creates a store there.
- It throws `InvalidFormat` when the directory holds none of those files and holds an entry whose name is not on the list of D9. It throws `Locked` as `open` does.

**A4. `close`.** `close(): void`. Functional items: F6, F24, F61, F70.
- On the store's own handle it does three things. It waits for each `storeBlobAsync` that was called before it. It syncs the store as `sync` does. It closes the store for this handle and for each view. When that sync fails, it throws the sync's error, and the store is closed all the same.
- On a view it ends the view's subscriptions. It closes that view and nothing else. It syncs nothing, and it does not throw.
- A second `close` does nothing and does not throw.

**A5. `isClosed`.** `isClosed(): boolean`. Functional items: F6, F24.
- It is true on a closed view. It is true for every handle after the store's own handle was closed. It is false otherwise. It does not throw.

**A6. `view`.** `view(branchName: string): JsStore`. Functional items: F24.
- `branchName` is the name of a branch that exists. It throws `BranchNotFound` for another name.
- It returns a new handle that is bound to that branch for its whole life. The handle is bound to the branch itself, and not to the name. Suppose the branch is deleted and another branch takes the name. The view does not act on the new branch.
- A deleted branch makes its view throw `BranchNotFound`, in each method that depends on a branch. Limit today: L4.

**A7. `pinnedBranch`.** `pinnedBranch(): string | null`. Functional items: F24.
- It is the name of the view's branch as it was when the view was made, and null on the store's own handle. It does not throw, also on a closed handle.

**A8. `recovery`.** `recovery(): JsRecoveryReport | null`. Functional items: F67.
- `JsRecoveryReport` holds four things. The answer is null when the last open found none of them.

**A9. `snapshotFailures`.** `snapshotFailures(): Array<JsSnapshotFailure>`. No functional item needs it.
- After a change of a state, the store can add a short cut of its own. The store's newest attempt for a state on a branch can fail. The answer then has one entry for that state and branch. Only attempts since the store was opened count. It is an empty list when there is none.
- The entry goes away when the store saves a short cut of that state on that branch. The entry also goes away if no short cut is due at a later change.

**A10. `append`.** `append(recordType: string, payload: Buffer): JsRecord`. Functional items: F7, F8, F10.
- `recordType` is any text of at most 65,535 bytes but `state_update`. `payload` is stored as the bytes that it is.
- The record goes to the handle's branch. The answer is the new record.
- It throws `InvalidOperation` for the type `state_update`.
- Limit today: L2.

**A11. `appendJson`.** `appendJson(recordType: string, data: any): JsRecord`. Functional items: F7, F8, F10.
- `recordType` is as for `append`. `data` is stored as its JSON text in UTF-8, so the record's `payload` is those bytes.
- The record goes to the handle's branch. The answer is the new record.
- It throws `InvalidOperation` for the type `state_update`.

**A12. `getRecord`.** `getRecord(id: string): JsRecord | null`. Functional items: F9, F11.
- `id` is a record's ID: a whole number from 0 to 2^64 - 1 in decimal digits. It throws `InvalidOperation` for another text.
- It is the record with that ID, from any branch. It is null when no record has the ID.

**A13. `getRecords`.** `getRecords(ids: Array<string>): Array<JsRecord | undefined | null>`. Functional items: F11.
- Each entry of `ids` is a record's ID, as for `getRecord`. It throws `InvalidOperation`, and reads nothing, when an entry is another text. The message names that entry.
- The answer has one entry for each entry of `ids`, in the same order. An entry is the record, or null when no record has that ID. An ID can be in `ids` more than one time.

**A14. `getStateUpdateJson`.** `getStateUpdateJson(id: string): string | null`. Functional items: F78.
- `id` is a record's ID, as for `getRecord`.
- It is null when no record has the ID, and when the record's type is not `state_update`.
- Otherwise it is a JSON text of an object with the keys `record_id`, `global_sequence`, `state_id`, `prev_update_offset`, `operation` and `timestamp`. `operation` is an object with one key, which names the kind of change.
- Where the change holds a value, the text shows the value's bytes as an array of numbers, one for each byte. So a caller gets an appended item when it reads those bytes as UTF-8 and then as JSON.
- `record_id` in this object is not the record's ID: in release 0.7.0 it is 0. The record's ID is the `id` of the call.
- It throws when the record's type is `state_update` and its payload does not decode as a change of a state.

**A15. `getRecordIdsByType`.** `getRecordIdsByType(recordType: string): Array<string>`. Functional items: F15, F69.
- It is the ID of each record of that type on every branch, in rising order of the IDs as numbers. It is an empty list when the type has no record.

**A16. `storeBlob`.** `storeBlob(content: Buffer, contentType: string): string`. Functional items: F46, F48, F49.
- `contentType` is a text of at most 65,535 bytes. It throws `InvalidOperation` for a longer one, and stores nothing.
- It is the SHA-256 hash of `content`, as 64 hexadecimal digits in lower case.

**A17. `storeBlobAsync`.** `storeBlobAsync(content: Buffer, contentType: string, options?: JsStoreBlobOptions | undefined | null): Promise<string>`. Functional items: F46, F50, F51.
- It is the one method that does not hold the caller. It copies `content` and returns a promise.
- The promise resolves with the hash, as `storeBlob` gives it. If the storing of the bytes fails, the promise rejects with that error. The call itself throws when the handle is closed, and when the store's thread for these stores cannot start.
- `options.chunked` is false when left out. When true, the store cuts the content into parts and keeps the parts. When the cut leaves a content in one part, the store keeps the content whole.
- The stores run one at a time, in the order of the calls on the store's handle and its views.

**A18. `getBlob`.** `getBlob(hash: string): Buffer | null`. Functional items: F47, F49.
- `hash` is 64 hexadecimal digits. It throws `InvalidOperation` for another text.
- It is the bytes stored under that hash. It is null when the store holds no such blob. It does not give the content type.
- It throws when it reads a kept copy that is damaged or cut short.

**A19. `createBranch`.** `createBranch(name: string, from?: string | undefined | null): JsBranch`. Functional items: F17, F19, F20, F64.
- `name` is a name that no branch has. `from` is the name of the parent. When `from` is left out, the parent depends on the handle. The store's own handle uses the current branch. A view uses its own branch.
- The new branch starts at the parent's head, with each of the parent's states. The answer is the new branch.
- It throws `BranchNotFound` when no branch has the name in `from`. It looks for that first. Then it throws `BranchExists` when a branch has the name.

**A20. `createBranchWithStates`.** `createBranchWithStates(name: string, from: string | undefined | null, filter: JsStateFilter): JsBranch`. Functional items: F17, F19, F20.
- It is `createBranch`, but the new branch takes only the states that `filter` selects. A state that is not taken has no value on the new branch.

**A21. `createBranchAtWithStates`.** `createBranchAtWithStates(name: string, from: string, atSequence: number, filter: JsStateFilter): JsBranch`. Functional items: F18, F19, F20.
- It is `createBranchAt`, but the new branch takes only the states that `filter` selects. Each of them reads as it read on `from` at `atSequence`.

**A22. `createBranchAt`.** `createBranchAt(name: string, from: string, atSequence: number): JsBranch`. Functional items: F18, F19, F20.
- `from` is the name of the parent, and it is not optional. `atSequence` is a sequence of the parent from zero to its head.
- The new branch starts at that sequence. Each state starts as the parent had it at that sequence. The answer is the new branch.
- It throws `InvalidSequence` for a sequence above the parent's head, `BranchExists` and `BranchNotFound`.

**A23. `createEmptyBranch`.** `createEmptyBranch(name: string, from?: string | undefined | null): JsBranch`. Functional items: F17, F19, F20.
- It is `createBranch`, but the new branch takes no state. When `from` is left out on a view, the parent is the view's branch. It looks for a taken name before it looks for the parent, so it throws `BranchExists` first. Limit today: L3.

**A24. `switchBranch`.** `switchBranch(name: string): JsBranch`. Functional items: F23, F64.
- On the store's own handle, the named branch becomes the current branch. The answer is that branch.
- It throws `BranchNotFound` for a name that no branch has. On a view it throws `InvalidOperation`, whatever the name.

**A25. `currentBranch`.** `currentBranch(): JsBranch`. Functional items: F23, F24.
- On the store's own handle, it is the current branch. A view gives its own branch.

**A26. `listBranches`.** `listBranches(): Array<JsBranch>`. Functional items: F26.
- It is every branch of the store, from any handle. The order of the answer is not specified.

**A27. `deleteBranch`.** `deleteBranch(name: string): void`. Functional items: F25, F64.
- It deletes the branch with that name, from any handle.
- It throws `InvalidOperation` in three cases: for `main`, for the current branch, and for a view's own branch when called through that view. It throws `BranchNotFound` for a name that no branch has.

**A28. `registerState`.** `registerState(registration: JsStateRegistration): void`. Functional items: F27, F64.
- `registration.id` is the state's name. `registration.strategy` is one of the texts `snapshot`, `append_log` and `tree`.
- `deltaSnapshotEvery` is 100 for a list and 50 for a tree when left out. `fullSnapshotEvery` is 10 when left out. `initialValue` is taken and not used.
- It throws `InvalidOperation` for another `strategy`, and `StateExists` when the name is registered.

**A29. `updateStateStrategy`.** `updateStateStrategy(registration: JsStateRegistration): void`. No functional item needs it.
- It takes a state that is registered as a list or as a tree, and gives it new values of `deltaSnapshotEvery` and `fullSnapshotEvery`. `strategy` is the state's own kind: it does not change a state's kind. `initialValue` is not used.
- It is for a caller that registers its states at each start. If the state exists, `registerState` throws `StateExists`. This call then gives the state the caller's values.

**A30. `getState`.** `getState(stateId: string): Buffer | null`. Functional items: F30, F37.
- It is the state's value on the handle's branch, as bytes. It is null when the state has no value on that branch.
- For a list, the bytes are the JSON text of an array, unless a `setState` gave the list other bytes.

**A31. `getStateJson`.** `getStateJson(stateId: string): any | null`. Functional items: F30, F37.
- It is the state's value on the handle's branch, read as JSON. It is null when the state has no value on that branch, and also when the value is the JSON null.
- It throws `Deserialization` when the bytes are no JSON.

**A32. `setState`.** `setState(stateId: string, value: Buffer): JsRecord`. Functional items: F29, F30, F79.
- It sets a new whole value, `value`, for the state on the handle's branch. It takes a state of any kind, and a name that is not registered.
- The answer is the record of the change: its `recordType` is `state_update`.

**A33. `setStateJson`.** `setStateJson(stateId: string, value: any): JsRecord`. Functional items: F29, F30, F79.
- It is `setState` with the JSON text of `value` as the bytes.

**A34. `appendToState`.** `appendToState(stateId: string, item: Buffer): JsRecord`. Functional items: F29, F31.
- It adds `item` to the end of the list. `item` is the JSON text of one value, as bytes. The call does not check that.
- The answer is the record of the change.

**A35. `appendToStateJson`.** `appendToStateJson(stateId: string, item: any): JsRecord`. Functional items: F29, F31.
- It adds the JSON value `item` to the end of the list. The answer is the record of the change.

**A36. `appendToStateJsonWithIdentity`.** `appendToStateJsonWithIdentity(stateId: string, item: any, idField: string, sequenceField: string): JsRecord`. Functional items: F34.
- `item` is a JSON object. Before the item is stored, the key `idField` gets the change's record ID as a text, and the key `sequenceField` gets the change's sequence as a number. If the item already has a value at one of those keys, the new value replaces it. When the two keys are the same, the item holds the sequence there.
- The answer is the record of the change. If `item` is no JSON object, it throws and adds no record.

**A37. `appendManyToStateJson`.** `appendManyToStateJson(stateId: string, items: Array<any>): JsAppendedBatch`. Functional items: F32, F33.
- It adds the items to the end of the list, in the order given. This is one change, with one record and one sequence.
- The answer's `record` is that record. `firstOrdinal` is the index of the first item, and item number i of `items` is at `firstOrdinal + i`. This holds for a list whose value is a JSON array of JSON items. On another value, the batch uses the stored count. Its items then do not read by index.
- It throws `InvalidOperation`, and writes nothing, when `items` is empty.

**A38. `appendManyToStateJsonWithIdentity`.** `appendManyToStateJsonWithIdentity(stateId: string, items: Array<any>, idField: string, sequenceField: string): JsAppendedBatch`. Functional items: F32, F33, F34.
- It is `appendManyToStateJson`, and each item is a JSON object. As in `appendToStateJsonWithIdentity`, each item gets two values: the record's ID at the key `idField`, and its sequence at the key `sequenceField`. All items get the values of the same one record.
- It throws, and writes nothing, when `items` is empty, when an item is no JSON object, and when `idField` equals `sequenceField`.

**A39. `editStateItem`.** `editStateItem(stateId: string, index: number, newValue: Buffer): JsRecord`. Functional items: F35.
- `index` counts from zero. `newValue` is the JSON text of one value, as bytes. The call does not check that.
- The item at `index` gets the new value. The answer is the record of the change.
- It throws `InvalidOperation` when `index` is not below the list's number of items.

**A40. `redactStateItems`.** `redactStateItems(stateId: string, start: number, end: number): JsRecord`. Functional items: F36.
- It removes the items from index `start` up to index `end`. The item at `end` stays. Each later item moves down. A part of the range that is beyond the list's end is ignored, and a `start` that is not below `end` removes nothing.
- The answer is the record of the change, also when it removed nothing.

**A41. `getStateLen`.** `getStateLen(stateId: string): number | null`. Functional items: F37.
- It is the list's number of items on the handle's branch, zero for a list with no items, and null when the state has no value on that branch.
- Limit today: L11.

**A42. `stateHeadOffset`.** `stateHeadOffset(stateId: string): number | null`. Functional items: F41.
- It is a number for the state's value on the handle's branch, or null when the state has no value there.
- The number changes when that state changes on that branch. It also changes when the store adds a record of its own for that state on that branch. Nothing else changes it. Two handles of one branch get the same number, and two branches that share an unchanged value get the same number.
- The number is a position in the log: the position of the state's newest record. A caller uses it only to compare.

**A43. `getStateSlice`.** `getStateSlice(stateId: string, offset: number, limit: number): Buffer | null`. Functional items: F37.
- `offset` is the index of the first item, from zero. `limit` is the largest number of items.
- It is the JSON text of an array of those items, as bytes. Each item is the list's own JSON value. Its keys are in the order written (N9). The spacing inside an item can differ from bytes that a caller gave. It is null when the state has no value on the handle's branch.

**A44. `getStateItemJson`.** `getStateItemJson(stateId: string, index: number): any | null`. Functional items: F37.
- `index` counts from zero. It is the item at `index`, as a JSON value.
- It is null for an index below zero, for an index that the list does not have, when the state has no value on the handle's branch, and when the value is no JSON array. Limit today: L4.
- It throws `Deserialization` when the state's bytes are no JSON. Bytes of length zero read as a list with no items.
- One case is apart: for the last index by the list's stored count, it can answer from the newest change alone, with no read of the earlier items.

**A45. `getStateItemsJson`.** `getStateItemsJson(stateId: string, ordinals: Array<number>): Array<any | undefined | null>`. Functional items: F37.
- The answer has one entry for each entry of `ordinals`, in the same order. An entry is the item at that index, as `getStateItemJson` gives it. An index can be there more than one time.
- Each entry is from one version of the list. With no index of zero or more in `ordinals`, it reads nothing and gives null for each entry.
- Limits today: L4 and L11.

**A46. `getStateTail`.** `getStateTail(stateId: string, count: number): Buffer | null`. Functional items: F37.
- It is the JSON text of an array, as bytes. The array holds the last `count` items, or every item when the list has fewer. It is null when the state has no value on the handle's branch.

**A47. `setAutoSnapshot`.** `setAutoSnapshot(enabled: boolean): void`. No functional item needs it.
- `false` stops the store from adding short cuts of its own at changes of states. It does so for the whole store. `true` lets it add them again. It is `true` when a store is opened.

**A48. `autoSnapshotEnabled`.** `autoSnapshotEnabled(): boolean`. No functional item needs it.
- It is the value that `setAutoSnapshot` set last, or `true`.

**A49. `compactState`.** `compactState(stateId: string): JsRecord | null`. No functional item needs it.
- It adds a short cut of the state's whole value on the handle's branch, as one record. The answer is that record, or null when the state has no value there. The value does not change.

**A50. `compactAllStates`.** `compactAllStates(): number`. No functional item needs it.
- It does `compactState` for each registered state on the handle's branch. The answer is the number of states for which it added a record.

**A51. `getCompactionSummary`.** `getCompactionSummary(): JsCompactionSummary`. No functional item needs it.
- It reports the numbers of `JsCompactionSummary` for the registered states on the handle's branch.

**A52. `stats`.** `stats(): JsStoreStats`. No functional item needs it.
- It reports sizes of the whole store, from any handle.

**A53. `appendWithLinks`.** `appendWithLinks(recordType: string, payload: Buffer, options: JsRecordOptions): JsRecord`. Functional items: F7, F12.
- It is `append`, and it also sets the record's two lists of links from `options.causedBy` and `options.linkedTo`. Each entry is a record's ID. If an entry is no ID, the call leaves it out and does not throw.
- Each list has at most 65,535 entries. Limit today: L2.

**A54. `appendJsonWithLinks`.** `appendJsonWithLinks(recordType: string, data: any, options: JsRecordOptions): JsRecord`. Functional items: F7, F12.
- It is `appendJson`, with the two lists of links as in `appendWithLinks`.

**A55. `query`.** `query(filter: JsQueryFilter): Array<JsRecord>`. Functional items: F13, F14.
- It gives records that were appended on the handle's branch, as `JsQueryFilter` says. They come in rising order of sequence, or in falling order when `reverse` is true.
- With no `limit`, the answer holds at most 10,000 records. `offset` leaves out that many matching records from the start of the answer.
- Limits today: L1 and L10.

**A56. `listStates`.** `listStates(): Array<JsStateInfo>`. Functional items: F28.
- It gives one entry for each registered state. `itemCount` and `opsSinceSnapshot` are for the handle's branch. The order of the answer is not specified.

**A57. `currentSequence`.** `currentSequence(): number`. Functional items: F16.
- It gives the handle's branch head.

**A58. `getStateAt`.** `getStateAt(stateId: string, atSequence: number): Buffer | null`. Functional items: F40.
- It gives the state's value at `atSequence` on the handle's branch, as bytes. It is the value after all changes with a sequence of at most `atSequence`. It is null when the state had no value then.

**A59. `getStateJsonAt`.** `getStateJsonAt(stateId: string, atSequence: number): any | null`. Functional items: F40.
- It is `getStateAt`, read as JSON. It throws `Deserialization` when the bytes are no JSON.

**A60. `getEffects`.** `getEffects(recordId: string): Array<string>`. Functional items: F12.
- `recordId` is a record's ID, as for `getRecord`. It throws `InvalidOperation` for another text.
- It is the ID of each record, on any branch, whose `causedBy` names that record. It is an empty list when there is none.

**A61. `getLinksTo`.** `getLinksTo(recordId: string): Array<string>`. Functional items: F12.
- It is `getEffects` for `linkedTo`.

**A62. `sync`.** `sync(): void`. Functional items: F61, F70.
- It puts each record that the store has accepted onto the disk, and saves the store's tables of states and branches. A failure to save the indexes on fields is only logged.
- It throws `DurabilityLost` when a flush of the log has failed since the store was opened.

**A63. `subscribe`.** `subscribe(config?: JsSubscriptionConfig | undefined | null): string`. Functional items: F52, F57.
- With no `config`, the subscription follows no kind of change and names no start sequence. Its buffer holds 1,000 events, and its size for a state in a catch-up is 10 MiB.
- The answer is the subscription's ID: a whole number in decimal digits, as a text. The subscription belongs to the handle that made it.

**A64. `unsubscribe`.** `unsubscribe(subscriptionId: string): void`. Functional items: F60.
- It ends the subscription. It also removes the waiting events, when it is called through the subscription's own handle. A later poll through that handle then throws `SubscriptionNotFound`.
- It does not throw for an ID that names no subscription. It throws `InvalidOperation` for a text that is no ID.

**A65. `catchUpSubscription`.** `catchUpSubscription(subscriptionId: string): void`. Functional items: F53, F54, F56, F58.
- It sends the subscription two things from the handle's branch: the followed states and the followed records. `JsSubscriptionConfig.fromSequence` says which. Then it sends the event `caught_up`, and it lets live events through. With no `fromSequence` it sends only `caught_up`.
- The replay of records starts with the record at `fromSequence` itself. The replayed records include the changes of states. If the subscription's types let it through, each change comes as a `record` event with the `record_type` `state_update`. The `payload` of such an event is null, since the stored change is no JSON. `getStateUpdateJson` reads the change.
- It can be called through any handle of the store. It throws `SubscriptionDropped` for a subscription that the store does not hold.
- Limits today: L5, L6, L7 and L8.

**A66. `pollSubscription`.** `pollSubscription(subscriptionId: string): JsStoreEvent | null`. Functional items: F55, F59, F60.
- It gives the next event that waits for the subscription, or null when none waits. It does not hold the caller.
- It works only for subscriptions made through this handle. It throws `SubscriptionNotFound` for another ID, and `InvalidOperation` for a text that is no ID.
- After a subscription has ended, the poll still gives its waiting events first. Then it throws `StoreClosed` when the store is closed, and `SubscriptionDropped` otherwise.

**A67. `pollSubscriptionTimeout`.** `pollSubscriptionTimeout(subscriptionId: string, timeoutMs: number): JsStoreEvent | null`. Functional items: F55, F59, F60.
- It is `pollSubscription`, with one difference. If no event waits, it holds the caller for up to `timeoutMs` milliseconds. If an event comes in that time, it gives that event.

**A68. `subscriptionCount`.** `subscriptionCount(): number`. No functional item needs it.
- It is the number of subscriptions that the store holds, from any handle.

**A69. `treeSet`.** `treeSet(stateId: string, path: string, entry: JsTreeEntry): JsRecord`. Functional items: F29, F38, F45.
- It sets a tree entry on the handle's branch: the entry for `path`. The answer is the record of the change.
- It throws `InvalidOperation` when the state is not registered as a tree.

**A70. `treeRemove`.** `treeRemove(stateId: string, path: string): JsRecord`. Functional items: F38, F45.
- It removes `path` from the tree on the handle's branch. The answer is the record of the change. It throws as `treeSet` does.

**A71. `treeBatch`.** `treeBatch(stateId: string, ops: Array<JsTreeOp>): JsRecord`. Functional items: F38, F45.
- It makes all changes of `ops`, in order. Together they are one change with one record. The tree gets all of them or none.
- It throws `InvalidOperation` as `treeSet` does. It also throws it when an entry's `op` is neither `set` nor `remove`, and when a `set` has no `entry`.

**A72. `treeGet`.** `treeGet(stateId: string, path: string): JsTreeEntry | null`. Functional items: F38.
- It is the entry of `path` in the tree on the handle's branch, or null when the tree has no such path.

**A73. `treeList`.** `treeList(stateId: string, prefix?: string | undefined | null): Array<JsTreeListEntry>`. Functional items: F38.
- It gives each path of the tree on the handle's branch with its entry, or only the paths that start with `prefix`.

**A74. `treeDiff`.** `treeDiff(stateId: string, fromSequence: number, toSequence: number): Array<JsTreeChange>`. Functional items: F39.
- It compares the tree at `fromSequence` with the tree at `toSequence` on the handle's branch, and gives one entry for each path that differs.

**A75. `treeSnapshot`.** `treeSnapshot(stateId: string): JsRecord | null`. No functional item needs it.
- It is `compactState` for a tree.

**A76. `registerStateFieldIndex`.** `registerStateFieldIndex(stateId: string, field: string, kind: string): void`. Functional items: F42, F43.
- `field` is a JSON pointer into each item of the list, such as `/timestamp`. `kind` is the text `number` or the text `string`.
- The index is for the handle's branch. If the index is there and current, a second call does nothing.
- It throws `InvalidOperation` for another `kind`.

**A77. `queryStateIndexRange`.** `queryStateIndexRange(stateId: string, field: string, opts: JsIndexRangeQuery): Array<number> | null`. Functional items: F42, F43.
- It is the indexes of the items with a numeric field value from `opts.gte` to `opts.lte`, both included. A bound that is left out is open.
- The answer is in rising order of the field, or in falling order when `opts.reverse` is true. `opts.offset` leaves out that many from its start, and `opts.limit` is its largest length.
- It is null if the handle's branch has no `number` index on that field. An empty list means that the index is there and no item matches.

**A78. `queryStateIndexEq`.** `queryStateIndexEq(stateId: string, field: string, value: string, opts: JsIndexEqQuery): Array<number> | null`. Functional items: F42, F43.
- It is the indexes of the items with the field value `value`. `opts.offset` and `opts.limit` work as in `queryStateIndexRange`.
- It is null if the handle's branch has no `string` index on that field.

**A79. `getStateIndexValueCounts`.** `getStateIndexValueCounts(stateId: string, field: string): Array<JsValueCount> | null`. Functional items: F42, F43.
- It gives each text of the field in the list, with the count of items for that text. It is null if the handle's branch has no `string` index on that field.

#### Types

**T1. `JsStoreBlobOptions`.** `{ chunked?: boolean }`.
- `chunked`: True cuts the content into parts and keeps the parts. When the cut leaves a content in one part, the store keeps the content whole. False when left out.

**T2. `JsRecord`.** `{ id: string; sequence: number; recordType: string; payload: Buffer; timestamp: number; causedBy: Array<string>; linkedTo: Array<string> }`.
- `id`: The record's ID: a whole number in decimal digits, as a text.
- `sequence`: The record's sequence on its branch.
- `recordType`: The record's type.
- `payload`: The record's payload, as the bytes that were stored.
- `timestamp`: The time when the store accepted the record, in microseconds since the start of 1970, UTC.
- `causedBy`: The IDs of the records that caused this record, as texts, in the order given at the append.
- `linkedTo`: The IDs of the records that this record is related to in another way, as texts, in the order given at the append.

**T3. `JsAppendedBatch`.** `{ record: JsRecord; firstOrdinal: number }`.
- `record`: The one record of the change.
- `firstOrdinal`: The index of the change's first item in the list.

**T4. `JsBranch`.** `{ id: string; name: string; head: number; parentId?: string; branchPoint?: number; created: number }`.
- `id`: The branch's ID: a whole number in decimal digits, as a text. `main` has the ID 1. Each new branch gets the next number of a counter that only rises. So reusing a deleted branch's name gives a branch with another ID. The counter is saved with the table of branches. After a crash, the store reads the branch IDs in its log and in its table of states, and sets the counter above each of them.
- `name`: The branch's name.
- `head`: The branch's head.
- `parentId`: The ID of the branch's parent, also after the parent is deleted. Absent for `main`.
- `branchPoint`: The branch point. Absent for `main`.
- `created`: The time when the branch was created, in microseconds since the start of 1970, UTC.

**T5. `JsStateFilter`.** `{ stateIds?: Array<string>; statePrefixes?: Array<string> }`.
- `stateIds`: Names of states that the new branch takes.
- `statePrefixes`: Each state whose name starts with one of these texts is taken too.

**T6. `JsStoreStats`.** `{ recordCount: number; blobCount: number; branchCount: number; stateSlotCount: number; totalSizeBytes: number; blobSizeBytes: number }`.
- `recordCount`: How many records the store holds, on every branch.
- `blobCount`: How many blobs the store holds.
- `branchCount`: How many branches the store has.
- `stateSlotCount`: How many states are registered.
- `totalSizeBytes`: The size of the log and of the blobs together, in bytes.
- `blobSizeBytes`: The size of the blobs, in bytes.

**T7. `JsRecoveryReport`.** `{ truncatedAt: number; droppedBytes: number; validRecords: number; unregisteredStates: Array<JsUnregisteredState>; undecodableStateUpdates: number; updatesOnMissingBranches: number }`.
- `truncatedAt`: The byte offset at which the open cut the log. When the open cut nothing, it is the log's length.
- `droppedBytes`: How many bytes the open removed from the end of the log. Zero when it removed none.
- `validRecords`: How many records the log holds after the open.
- `unregisteredStates`: Each state whose changes the open brought back although the store holds no registration for it.
- `undecodableStateUpdates`: The number of `state_update` records that the open passed over, because they do not decode.
- `updatesOnMissingBranches`: How many changes of states the open passed over because their branch is not in the store.

**T8. `JsUnregisteredState`.** `{ id: string; updates: number }`.
- `id`: The state's name.
- `updates`: How many of its changes the open brought back.

**T9. `JsSnapshotFailure`.** `{ stateId: string; branch: string; failures: number; sequence: number; error: string }`.
- `stateId`: The state's name.
- `branch`: The name of the branch.
- `failures`: How many changes in a row found a short cut due and failed to save it. When changes of one state race, the number can be lower.
- `sequence`: The sequence of the newest such change.
- `error`: The error of the newest attempt, as a text.

**T10. `JsStoreConfig`.** `{ path: string; blobCacheSize?: number }`.
- `path`: The store's directory.
- `blobCacheSize`: How many blobs the open store keeps in its memory. 1000 when left out, and at least one.

**T11. `JsStateRegistration`.** `{ id: string; strategy: string; deltaSnapshotEvery?: number; fullSnapshotEvery?: number; initialValue?: Buffer }`.
- `id`: The state's name.
- `strategy`: The state's kind: `snapshot`, `append_log` or `tree`.
- `deltaSnapshotEvery`: For a list: the store adds a small short cut when this many changes have come since its last short cut. The small short cut holds the items since then. An edit, a redact or a set pauses the small short cuts. They start again after the list's next short cut. A whole short cut can be due at the same change. The store then adds the whole one and no small one. 100 for a list and 50 for a tree when left out. For a tree, the source has a rule of the same form.
- `fullSnapshotEvery`: For a list: the store adds a short cut of the whole value at a certain count. The count must reach the larger of two numbers. The count is of the changes since the last whole one. The first is `deltaSnapshotEvery` times `fullSnapshotEvery`. The second is the number of items at the last whole one. For a new list it is zero. The store does not keep that count. It takes the number of small short cuts since the last whole one, and multiplies it by today's `deltaSnapshotEvery`. Then it adds the changes since the last small one. So the count is off after `updateStateStrategy` changes `deltaSnapshotEvery`. 10 when left out.
- `initialValue`: Taken and not used.

**T12. `JsBranchGcOptions`.** `{ deleteOrphaned?: boolean; deleteEmpty?: boolean; deleteStaleOlderThan?: number; namePatterns?: Array<string>; force?: boolean; reparentTo?: string }`. No method takes it or gives it.

**T13. `JsBranchGcResult`.** `{ deleted: Array<string>; skipped: Array<string>; reparented: number }`. No method takes it or gives it.

**T14. `JsCompactionSummary`.** `{ totalOperations: number; compactableOperations: number; totalBytes: number; compactableBytes: number; statesNeedingCompaction: number }`.
- `totalOperations`: In the source's words: "Total number of state operations across all states."
- `compactableOperations`: In the source's words: "Operations that could be skipped after compaction."
- `totalBytes`: In the source's words: "Total bytes used by state operations."
- `compactableBytes`: In the source's words: "Bytes that could be reclaimed (logically) after compaction."
- `statesNeedingCompaction`: In the source's words: "Number of states that would benefit from compaction."

**T15. `JsRecordOptions`.** `{ causedBy?: Array<string>; linkedTo?: Array<string> }`.
- `causedBy`: The IDs of the records that caused the new record, as texts.
- `linkedTo`: The IDs of records related to the new record in another way, as texts.

**T16. `JsQueryFilter`.** `{ types?: Array<string>; fromSequence?: number; toSequence?: number; limit?: number; offset?: number; reverse?: boolean }`.
- `types`: Only records of these types. Every type when left out.
- `fromSequence`: The first sequence of the range, included. When left out, it is the branch's start.
- `toSequence`: The last sequence of the range, included. The branch's head when left out.
- `limit`: The largest number of records in the answer. 10,000 when left out.
- `offset`: How many matching records to leave out from the start of the answer. Zero when left out.
- `reverse`: True gives the records in falling order of sequence. False when left out.

**T17. `JsIndexRangeQuery`.** `{ gte?: number; lte?: number; limit?: number; offset?: number; reverse?: boolean }`.
- `gte`: The lowest number, included. Open when left out.
- `lte`: The highest number, included. Open when left out.
- `limit`: The largest length of the answer.
- `offset`: How many to leave out from the start of the answer.
- `reverse`: True gives the answer in falling order of the field.

**T18. `JsIndexEqQuery`.** `{ limit?: number; offset?: number }`.
- `limit`: The largest length of the answer.
- `offset`: How many to leave out from the start of the answer.

**T19. `JsValueCount`.** `{ value: string; count: number }`.
- `value`: A text that the field has.
- `count`: How many items have it.

**T20. `JsStateInfo`.** `{ id: string; strategy: string; itemCount?: number; opsSinceSnapshot: number }`.
- `id`: The state's name.
- `strategy`: The state's kind as a text: `snapshot`, `append_log` or `tree`. The Rust interface's two other kinds read `delta` and `struct`.
- `itemCount`: The number of items that the store has counted for the state on the handle's branch. Absent when the state has no value there.
- `opsSinceSnapshot`: A count of changes of the state on the handle's branch. It starts again at each short cut of the state, small or whole. Zero when the state has no value there.

**T21. `JsSubscriptionConfig`.** `{ bufferSize?: number; maxSnapshotBytes?: number; fromSequence?: number; filter?: JsSubscriptionFilter }`.
- `bufferSize`: How many events can wait for the subscription before the store drops it. 1,000 when left out.
- `maxSnapshotBytes`: The largest size of a state that a catch-up sends whole, in bytes. 10 MiB when left out.
- `fromSequence`: The sequence from which a catch-up sends states and records. With none, a catch-up sends neither.
- `filter`: What the subscription follows. With none, it follows nothing.

**T22. `JsSubscriptionFilter`.** `{ recordTypes?: Array<string>; branch?: string; stateIds?: Array<string>; includeRecords?: boolean; includeStateChanges?: boolean; includeBranchEvents?: boolean }`.
- `recordTypes`: Only records of these types. Every type when left out.
- `branch`: A branch's name. The store takes it and does not apply it yet.
- `stateIds`: Only these states. When left out, it means every state.
- `includeRecords`: True follows records. False when left out.
- `includeStateChanges`: True follows changes of states. False when left out.
- `includeBranchEvents`: True follows new heads, created branches and deleted branches. False when left out.

**T23. `JsStoreEvent`.** `{ eventType: string; data: string }`.
- `eventType`: One of the texts `record`, `state_snapshot`, `state_delta`, `branch_head`, `branch_created`, `branch_deleted`, `caught_up` and `dropped`.
- `data`: The event as a JSON text of an object. Its key `type` holds the same text as `eventType`, and its other keys are in snake case. A `record` event has `record`. Its fields are `id` as a number, `sequence`, `branch`, `record_type`, `timestamp`, `payload_size` and `payload`. That `payload` is the record's payload read as JSON. This needs a payload that is JSON and has at most 4,096 bytes. Otherwise it is null. A `state_snapshot` event has `state_id`, `data`, `sequence`, `truncated`, `total_bytes`, `from_index` and `total_length`. A `state_delta` event has `state_id`, `operation` and `sequence`, with `operation` in the form that A14 gives. A `branch_head` event has `branch` and `head`. A `branch_created` event has `branch`. A `branch_deleted` event has `name`. A `dropped` event has `reason`.

**T24. `JsTreeEntry`.** `{ blobHash: string; size: number; mode: number }`.
- `blobHash`: The hash of the blob that the entry names, as a text.
- `size`: A size in bytes, as the caller gave it.
- `mode`: A file mode, as the caller gave it.

**T25. `JsTreeOp`.** `{ op: string; path: string; entry?: JsTreeEntry }`.
- `op`: The text `set` or the text `remove`.
- `path`: The path.
- `entry`: The new entry. A `set` needs it.

**T26. `JsTreeChange`.** `{ changeType: string; path: string; oldEntry?: JsTreeEntry; newEntry?: JsTreeEntry }`.
- `changeType`: The text `added`, `modified` or `removed`.
- `path`: The path.
- `oldEntry`: The entry at the first sequence. Absent for `added`.
- `newEntry`: The entry at the second sequence. Absent for `removed`.

**T27. `JsTreeListEntry`.** `{ path: string; blobHash: string; size: number; mode: number }`.
- `path`: The path.
- `blobHash`: As in `JsTreeEntry`.
- `size`: As in `JsTreeEntry`.
- `mode`: As in `JsTreeEntry`.

#### Failure codes

**E1. The codes.** A failure that the store reports has a `code` that is one of these 23 texts: `BlobNotFound`, `BranchExists`, `BranchNotFound`, `ChecksumMismatch`, `Corruption`, `Deserialization`, `DurabilityLost`, `HashMismatch`, `InvalidFormat`, `InvalidOperation`, `InvalidSequence`, `Io`, `Locked`, `NotInitialized`, `RecordNotFound`, `Serialization`, `StateExists`, `StateNotRegistered`, `StoreClosed`, `StrategyMismatch`, `SubscriptionDropped`, `SubscriptionNotFound`, `Transaction`.

**E2. The messages.** When the store itself reports a failure, the message has a fixed form for its code. `…` stands for a part that differs from failure to failure.
- `Io`: `IO error: …`. The rest is the text of the error below. For a plain error of the operating system, that text ends with `(os error N)`. N is the error's number.
- `DurabilityLost`: `Durability lost: …`.
- `RecordNotFound`: `Record not found: …`.
- `BranchNotFound`: `Branch not found: …`.
- `BlobNotFound`: `Blob not found: …`.
- `StateNotRegistered`: `State not registered: …`.
- `StateExists`: `State already exists: …`. The rest is the state's name, and nothing follows it.
- `InvalidSequence`: `Invalid sequence: … (head is …)`.
- `BranchExists`: `Branch already exists: …`.
- `Serialization`: `Serialization error: …`.
- `Deserialization`: `Deserialization error: …`.
- `Corruption`: `Corruption detected: …`.
- `StrategyMismatch`: `State strategy mismatch: expected …, got …`.
- `Locked`: `Store is locked by another process`. This is the whole message.
- `NotInitialized`: `Store not initialized`.
- `InvalidFormat`: `Invalid store format: …`.
- `ChecksumMismatch`: `Checksum mismatch: expected …, got …`.
- `HashMismatch`: `Hash mismatch: expected …, got …`.
- `Transaction`: `Transaction error: …`.
- `InvalidOperation`: `Invalid operation: …`.
- `SubscriptionDropped`: `Subscription was dropped`.
- `StoreClosed`: `Store has been closed`, or `View has been closed` from a view that was closed.
- `SubscriptionNotFound`: `Subscription not found`.
- The binding makes some failures itself, with the code `InvalidOperation`, `BranchNotFound`, `Deserialization` or `Serialization`. Such a message is the binding's own text or the text of the JSON error. Two examples of the binding's text are `Invalid record ID` and `Branch not found`. It does not always start as the list above says.
- These are the texts of release 0.7.0. Release 0.6.0 puts no code of E1 on a failure. So a caller that runs on that release tells failures apart by its texts. Some of them differ. In 0.6.0, the text for a dropped subscription is `Subscription disconnected`.

### The store's files

Other programs read a store's files beside the package: an older release of the package, the five Python tools in `tools/`, and programs outside this repository. D1, D2 and D8 state the bytes. D3 to D6 say what each file holds, and do not state its bytes yet.

**D1. `MANIFEST`.** The file is the four bytes `RST\0`, then one byte for the format version, which is 1 or 2. In version 2, eight bytes follow. They hold a set of feature bits, as a number of 64 bits with its lowest byte first. Bit 0 says that the store holds a change of several items of a list. A new store has version 1. The store writes version 2, with bit 0 set, just before its first change of several items of a list. If that change then fails, the file stays at version 2. A build opens version 1. It opens version 2 if it knows every set bit. It refuses each other version with `InvalidFormat`, and it refuses a bit that it does not know in the same way. A release from before such changes refuses version 2. Functional items: F71.

**D2. `records.log`.** The log of records, in the order in which the store accepted them. Each record is these bytes, in this order. Each number is unsigned and has its lowest byte first, unless its line says otherwise. Functional items: F7 to F9, F65, F66 and F68.
- The four bytes `REC\0`, one byte for the version, which is 1, and one byte of flags, which is 0.
- The record's ID, its sequence and the ID of its branch, each in 8 bytes.
- The time in microseconds since the start of 1970, UTC, as a signed number in 8 bytes.
- The length of the type in 2 bytes, then the type in UTF-8.
- One byte for the payload's encoding: 0 for JSON, 1 for MessagePack and 2 for raw bytes.
- The length of the payload in 4 bytes, then the payload.
- The number of entries of `causedBy` in 2 bytes, then each ID in 8 bytes. Then the same for `linkedTo`.
- The CRC-32 of the payload alone, in 4 bytes. Limit today: L9.

**D3. `state.bin`.** It holds each registration. It also holds one place in the log for each branch and each state: the state's newest change. If the contents changed, a save writes a new file, and the new file replaces the old one. Functional items: F27, F64 and F66.

**D4. `state-indexes.bin`.** The indexes on fields. They are made from the lists, so a lost file loses no data. Functional items: F42 and F43.

**D5. `branches.bin`.** The branches and the current branch. If the contents changed, a save writes a new file, and the new file replaces the old one. Functional items: F23, F26 and F64.

**D6. `blobs/`.** One file for each blob, under its hash. A content that is kept in parts has a file that names its parts in order, and each part is a blob of its own. Functional items: F46 to F51 and F71.

**D7. `LOCK`.** The store holds this file locked while it is open. It is a regular file, or absent. Functional items: F5.

**D8. A change of a state in the log.** It is a record of the type `state_update`. Functional items: F29, F66 and F78.
- From release 0.3.0 on, the record's encoding byte is 1. Its payload is a MessagePack map with the keys `record_id`, `global_sequence`, `state_id`, `prev_update_offset`, `operation` and `timestamp`.
- `prev_update_offset` is where this state's previous change is in the log, or nil. To read the state, the store follows these places back through the log.
- `operation` is a map with one key, which names the kind of change: `Set`, `Delta`, `Append`, `AppendMany`, `Redact`, `Edit`, `Snapshot`, `DeltaSnapshot`, `TreeSet`, `TreeRemove`, `TreeBatch`, `TreeDeltaSnapshot` or `Field`. The bytes of a value are stored as MessagePack `bin`.
- A build fails to decode a kind that it does not know. The source says of this: "A new variant is therefore a stored-format change too". Since release 0.3.0, one kind was added: `AppendMany`. The `MANIFEST` marks a store that holds one (D1).
- In a store that a release before 0.3.0 wrote, the encoding byte is 0 or 2. The payload is then the same map as JSON, with each byte of a value as a number. A store can hold both forms.
- A store from an old release can also hold a record of the type `state_update` that is no change of a state. An open passes it over and counts it.

**D9. Other entries in the store's directory.** No functional item needs this item.
- The store's own names in its directory are `MANIFEST`, `LOCK`, `records.log`, `state.bin`, `state-indexes.bin`, `branches.bin` and `blobs`, and each name that starts with `.chronicle-checkpoint-`. It makes a file of the last kind during a save and removes it.
- The store does not read, change or remove an entry with another name. It lists its directory in two cases only. The first is to remove files left by an interrupted save. The second is before `openOrCreate` creates a store. One case is apart: `MANIFEST`, `state.bin`, `state-indexes.bin` or `branches.bin` can be a symbolic link. A save then replaces the link's target file. The new file is in that file's own directory, and the link stays. The log is written through a link at `records.log`. A link at `LOCK` is refused (D7).
- `openOrCreate` creates a store beside these names and beside no other: `lost+found`, `.fseventsd`, `.Spotlight-V100`, `.Trashes`, `.TemporaryItems`, `System Volume Information`, `$RECYCLE.BIN`, `.DS_Store`, `LOCK`, `blobs`, and a name that starts with `.chronicle-checkpoint-`. The check reads names alone: a regular file with the name `blobs` passes it, and the create then fails with `Io`. So a caller puts its own files into the directory only after the store exists.

### The Rust interface

**R1. The crate.** The library is the Rust crate `chronicle` in `src/`. A caller takes it from the Git repository, since the name `chronicle` on crates.io is another project. Its entry is `Store`, with `StoreConfig` and `RecordInput`. The Node interface is a binding over it, in the same crate.

**R2. What the crate makes public.** The crate makes its modules public: `blobs`, `branches`, `error`, `records`, `state`, `store`, `subscriptions`, `types` and `wal`. This spec does not list the functions of `Store` or of these modules as items. The Node binding, the repository's tests and `examples/store_probe.rs` call the Rust interface.

## Where the code differs today

The code at `f4e39ac` does not meet these promises. Each limit has a label. An item names the label of each limit that touches it. Each limit in the list has evidence: a read of the code, a run, or a test of this repository. It is not complete: no one has yet held every functional item against the code by a test. A change that removes a limit removes its entry here, and its label at the items.

- **L1. A query with a filter by type can miss matching records (F13, F14, A55).** The code takes the first records of the range, and then keeps those of the named types. The number of records taken is 4 × (limit + records to skip) + 100. So the answer can hold fewer records than the limit, or none. Later records of the range can match all the same. `query_range_on` in `src/store.rs`.
- **L2. A record does not read back if one of its lists has more than 65,535 links, or if its type has more than 65,535 bytes (F12, A10, A53).** An append takes a list of any length. The log writes the list's length in 16 bits, and then writes every ID. No code checks the length. At the next open, such a record looks like a cut-off record, or the open refuses the store. `write_record` in `src/records/log.rs`.
- **L3. A new branch that takes no states gets the parent `main` in one case (F17, A23).** In that case, the create is on the store's own handle and names no parent. Every other create takes the caller's branch. `create_empty_branch` in `src/store.rs`.
- **L4. For an index below zero, two reads of items answer before they look for the store or for the branch (F6, F24, N8, A6, A44, A45).** So they answer null on a closed handle, and through a view whose branch is deleted. `getStateItemsJson` answers an empty list of indexes in the same way, with an empty list. `getStateItemJson` and `getStateItemsJson` in `src/napi.rs`.
- **L5. A record appended during a catch-up can miss the subscription both ways (F56, A65).** `catch_up_subscription_on` in `src/store.rs` replays from the log and then marks the subscription as caught up, and it takes no write lock between the two. An append takes the write lock, and its event passes over a subscription that is not yet caught up.
- **L6. A start sequence above the branch's head replays every record of the branch (F54, A65).** The code is `iter_from_on` in `src/store.rs` and the replay loop of the catch-up. The loop checks a record's branch and type, and not its sequence.
- **L7. A followed state whose bytes are no JSON is sent as no value, with no word that anything is missing (F54, A65).** This holds for a state that is no larger than the size of F58. A larger one comes marked as cut. The catch-up, in `src/store.rs`.
- **L8. A large state can be sent whole and marked as cut (F58, A65).** The catch-up compares the whole value's size with the limit. Then it adds up the sizes of the items, without the brackets and commas between them.
- **L9. A record's checksum covers its payload alone (F68, A2, D2).** So the open's scan does not notice damage to a record's ID, sequence, branch, type or links. `write_record` in `src/records/log.rs`.
- **L10. A query of a reversed range ends the process (F13, A55).** A reversed range has its first sequence after its last sequence. F13 promises an answer with no records. The call panics in the index's search, and the panic ends the Node process.
- **L11. A list's stored item count can be wrong when a release before 0.5.0 wrote the store (F28, F37, A41, A45).** The store counts the items again at the list's first change of several items after an open. It also counts them again at a request to compact. Until then `getStateLen` gives the stored number, and a read of several items by their indexes can disagree with the list. `tests/review7_fixes.rs` shows both.
