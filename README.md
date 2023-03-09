# Skyr

A domain-specific language for [Infrastructure As Code][iac], with a phased execution model,
allowing for expressive and highly dynamic IaC solutions.

[iac]: https://en.wikipedia.org/wiki/Infrastructure_as_code

## Disclaimer

Please note that this is a quick and dirty repo, very much in early development.
Don't use this for anything real!

## Usage

At its core, Skyr is a basic functional language:

```skyr
import List

ints = [12, 32, 43]

square = fn(n: Integer) => n * 2

squaredInts = List.map(ints, square)

type MyRecord = {
  ints: [Integer]
  string: String
}

value: MyRecord = {
  ints: squaredInts
  string: "Hello, Skyr"
}
```

It is strictly typed, with inference and basic parametric polymorphism.

```skyr
identity = fn(x) => x

string = identity("string")
integer = identity(123)
```

### Resources

What makes the language special, and domain-specific, are what're called resources.
Resources are expressions that represent a stateful resource external to the program.

Anything that can be interacted with using a CRUD (Create, Read, Update, Delete) interface
can be exposed as a resource in Skyr.

Here's an example from the standard library.

```skyr
import Random

Random.Identifier {
  name: "my-id"
  byteLength: 4
}
```

Here, we're defining that there should be a random identifier that can be used for
different purposes.

By giving the resource a name, the resulting state can be used in subsequent parts of
the code.

```skyr
import Random
import FileSystem

id = Random.Identifier {
  name: "my-id"
  byteLength: 4
}

FileSystem.File {
  path: "my-file.txt"
  content: id.hex
}
```

Here, we're generating a file called `my-file.txt` with the content filled with the
resulting generated ID in a hexadecimal represenation.

### Applying State

To apply a configuration, we use the Skyr CLI.

```shell
$ skyr apply
Create Random.Identifier(my-id) {
  name: "my-id"
  byteLength: 3
}

Continue? ▮
```

Skyr creates an action plan based on what it knows. In this case, we're not ready to
create the file because it's dependent on the identifier for its content. So, the first
phase creates the identifier. We enter `yes` in the prompt.

```shell
$ skyr apply
Create Random.Identifier(my-id) {
  name: "my-id"
  byteLength: 3
}

Continue? yes

Created Random.Identifier(my-id) in 164.583µs

Create FileSystem.File(my-file.txt) {
  path: "my-file.txt"
  content: "fa3a12"
}

Continue? ▮
```

After the identifier has been created, the file can be created, since it now has all the
values it depends on.
