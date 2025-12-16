defmodule OrchidStageTest do
  use ExUnit.Case, async: false # 因为涉及到 Registry 单例，建议先关掉 async
  alias Orchid.{Recipe, Param}

  # --- 辅助 Step 定义 ---

  defmodule Steps.Echo do
    use Orchid.Step
    def run(input, _opts), do: {:ok, Param.set_payload(input, "echo: #{Param.get_payload(input)}")}
  end

  defmodule Steps.Sleepy do
    use Orchid.Step
    # 模拟耗时操作
    def run(input, opts) do
      ms = Keyword.get(opts, :ms, 100)
      Process.sleep(ms)
      {:ok, input}
    end
  end

  defmodule Steps.Flaky do
    use Orchid.Step
    # 模拟不稳定：前 n 次失败，第 n+1 次成功
    # 我们利用 Agent 来存储状态（模拟外部数据库或服务）
    def run(input, _opts) do
      agent_pid = Param.get_payload(input)
      count = Agent.get_and_update(agent_pid, fn n -> {n, n + 1} end)

      if count < 2 do
        # 前两次失败
        {:error, "Service unavailable (attempt #{count + 1})"}
      else
        # 第三次成功
        {:ok, Param.new(:result, :string, "Success at attempt #{count + 1}")}
      end
    end
  end

  # --- 测试用例 ---

  setup do
    # 确保 Registry 启动。
    # 在实际应用中，这通常放在 application.ex 中。
    # start_link 如果已启动会返回 {:error, {:already_started, pid}}，这里忽略即可。
    case Registry.start_link(keys: :unique, name: Orchid.Registry) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      other -> other
    end
    :ok
  end

  test "Happy Path: 正常执行简单的线性任务" do
    input = Param.new(:text, :string, "hello")

    steps = [
      {Steps.Echo, :text, :output}
    ]
    recipe = Recipe.new(steps)

    # 使用 GenStage 执行器
    {:ok, results} = Orchid.run(recipe, [input],
      executor_and_opts: {OrchidStage, [worker_num: 1]}
    )

    assert Param.get_payload(results[:output]) == "echo: hello"
  end

  test "Concurrency: 多 Worker 并行执行应该比串行快" do
    # 定义 4 个并行的步骤，每个睡 100ms
    # 如果串行（Serial），总耗时应 > 400ms
    # 如果并行（Async/GenStage），总耗时应接近 100ms + 开销
    steps = [
      {Steps.Sleepy, :in1, :out1, [ms: 100]},
      {Steps.Sleepy, :in2, :out2, [ms: 100]},
      {Steps.Sleepy, :in3, :out3, [ms: 100]},
      {Steps.Sleepy, :in4, :out4, [ms: 100]}
    ]

    inputs = [
      Param.new(:in1, :nil, nil), Param.new(:in2, :nil, nil),
      Param.new(:in3, :nil, nil), Param.new(:in4, :nil, nil)
    ]
    recipe = Recipe.new(steps)

    {time_us, result} = :timer.tc(fn ->
      Orchid.run(recipe, inputs,
        # 开启 4 个 Worker
        executor_and_opts: {OrchidStage, [worker_num: 4]}
      )
    end)

    assert {:ok, _} = result

    time_ms = time_us / 1000
    IO.puts("Parallel Execution Time: #{time_ms}ms")

    # 预留一些调度开销，只要远小于 400ms 即可证明是并行的
    assert time_ms < 250
  end

  test "Retry: 自动重试机制能从临时故障中恢复" do
    # 启动一个 Agent 计数器，初始为 0
    {:ok, agent} = Agent.start_link(fn -> 0 end)

    input = Param.new(:counter_agent, :pid, agent)

    steps = [
      {Steps.Flaky, :counter_agent, :final_result}
    ]
    recipe = Recipe.new(steps)

    {:ok, results} = Orchid.run(recipe, [input],
      executor_and_opts: {OrchidStage, [
        retry: 3,       # 允许重试 3 次
        worker_num: 2
      ]}
    )

    # 验证最终结果
    assert Param.get_payload(results[:final_result]) == "Success at attempt 3"

    # 验证 Agent 被调用了 3 次 (0, 1 失败, 2 成功)
    assert Agent.get(agent, & &1) == 3
  end

  # test "Failure: 超过重试次数后应报错" do
  #   {:ok, agent} = Agent.start_link(fn -> 0 end)
  #   input = Param.new(:counter_agent, :pid, agent)

  #   steps = [{Steps.Flaky, :counter_agent, :final_result}]
  #   recipe = Recipe.new(steps)

  #   # 只允许重试 1 次（总共运行 2 次：初始 1 + 重试 1）
  #   # Flaky Step 需要第 3 次才能成功，所以这里应该失败
  #   result = Orchid.run(recipe, [input],
  #     executor_and_opts: {OrchidStage, [retry: 1]}
  #   )

  #   assert {:error, {:step_failed_exhausted, _, "Service unavailable (attempt 2)"}} = result
  # end
end
