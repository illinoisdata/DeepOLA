`NodeReader<T: Send>`

`Payload<T>` : EOF | data | extra signal
  - Process this data block

`DataBlock<T>`: group of rows to update

`Vec<T>`: to be inferred
  - T = ArrayRow: one row
  - contain last state

Interface
- Vec<T>, Vec<usize>