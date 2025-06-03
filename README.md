# 📜 Akasha

Akasha is an async, stream-based, functional database engine written in Rust.

All queries in Akasha are modeled as a **Transaction Monad**, which can be composed and chained functionally. Each query returns a `Transaction<Row<T>>` monadic wrapper around the rows of a result set.

When sequencing effects (like subqueries or IO), Monadic operations become async stream pipelines and simple row-level functions (like map) act directly on the data. This separation lets Akasha compile composable queries into efficient, lazy async streams.

---

# 📁 Theory

In category theory terms, Akasha uses a composition of functors:

$F = T \circ R$

Where:

* $T(X) = \text{Transaction}(X)$ models the transactional computation,
* $R(X) = \text{List}(X)$ models the row container,
* $X$ is the type of the data being queried

---

# 🔍 Example

```haskell
insert users {
    name = "John Doe",
    age = 35,
    email = "test@example.com"
}
```

```haskell
scan users
    |> filter (\u -> u.age > 34)
    |> map (\u -> (u.name, u.email))
```

The `scan` operation has the type:

$$
\texttt{scan users} : T(R(\text{User})) = T(R(\text{String} \times \text{Int} \times String))
$$

Each function in the pipeline is a transformation via monadic bind:

$$
T(R(X)) \xrightarrow{\text{bind}} T(R(Y))
$$

This allows composition of queries without leaving the monadic context, preserving purity, composability, and deferred execution.

---

# 🚣️ Roadmap

* [x] Buffer pooling
* [x] Memory-mapped files
* [x] Paged storage
* [x] Query parsing
* [x] Query compilation
* [x] Select query execution
* [x] Insert query execution
* [ ] Update query execution
* [ ] Delete query execution
* [ ] Indexing
  ...and more.