# [WIP]OrchidStage

**TODO: Add description**

## Installation

```elixir
def deps do
  [
    {:orchid_stage, git: "https://github.com/GES233/OrchidStage.git"}
  ]
end
```

## Usage

Just simply:

```elixir
res = Orchid.run(recipe, inputs, executor_and_opts: {OrchidStage, [worker_num: 4]})
```
