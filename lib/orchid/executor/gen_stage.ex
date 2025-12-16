defmodule Orchid.Executor.GenStage do
  @moduledoc """
  A simple GenStage-based executor.
  """
  @behaviour Orchid.Executor

  alias Orchid.Executor.GenStage.{Dispatcher, Worker}

  # 1. 入口：这就好比餐厅经理
  # 他不亲自做菜，他只负责组建团队，然后等着上菜。
  @impl true
  def execute(ctx, opts) do
    # 1.1 启动调度员 (Producer)
    # 我们把当前进程 (self()) 传给它，这样它做完饭能通知我们
    {:ok, producer_pid} = Dispatcher.start_link(ctx, self())

    # 1.2 启动工人 (Consumer)
    # 根据配置的并发数，启动若干个工人，让他们去我们要“监听”那个调度员
    concurrency = Keyword.get(opts, :concurrency, System.schedulers_online())

    for id <- 1..concurrency do
      Worker.start_link(producer_pid, id)
    end

    # 1.3 经理坐下喝茶，等待结果
    receive do
      {:recipe_done, {:ok, results}} -> {:ok, results}
      {:recipe_done, {:error, reason}} -> {:error, reason}

      #这是为了防止进程死锁设的超时，实际生产中可能不需要
      after 30_000 -> {:error, :timeout}
    end
  end

  # ===========================================================================

  # 2. 调度员 (Producer)
  # 他手里拿着菜单 (Context)，谁来要活，他就给谁派活。
  defmodule Dispatcher do
    use GenStage
    alias Orchid.Scheduler

    def start_link(initial_ctx, caller_pid) do
      GenStage.start_link(__MODULE__, {initial_ctx, caller_pid})
    end

    @impl true
    def init({ctx, caller_pid}) do
      # state 包含：
      # - ctx: 当前的烹饪进度
      # - caller: 等结果的经理
      # - demand: 目前工人们总共想要多少个活（库存的空闲人手）
      {:producer, %{ctx: ctx, caller: caller_pid, demand: 0}}
    end

    # A. 收到工人的“我要活” (Demand) 请求
    @impl true
    def handle_demand(incoming_demand, state) do
      # 累加需求，然后尝试派活
      new_state = %{state | demand: state.demand + incoming_demand}
      dispatch_events(new_state)
    end

    # B. 收到工人的“我做完了” (Result) 汇报
    @impl true
    def handle_cast({:step_finished, step_idx, result}, state) do
      case result do
        {:ok, outputs} ->
          # 1. 更新大管家的账本 (Context)
          new_ctx = Scheduler.merge_result(state.ctx, step_idx, outputs)

          # 2. 检查是不是全做完了
          if Scheduler.done?(new_ctx) do
            send(state.caller, {:recipe_done, {:ok, Scheduler.get_results(new_ctx)}})
            # 任务完成，停止自己
            {:stop, :normal, state}
          else
            # 3. 没做完，继续尝试派发新产生的任务
            dispatch_events(%{state | ctx: new_ctx})
          end

        {:error, reason} ->
          # 只要有一个步骤失败，全盘结束
          send(state.caller, {:recipe_done, {:error, reason}})
          {:stop, :normal, state}
      end
    end

    # C. 核心逻辑：派活
    defp dispatch_events(state) do
      # 1. 问 Scheduler：现在有哪些步骤是可以跑的？
      ready_steps = Scheduler.next_ready_steps(state.ctx)

      # 2. 看看有多少人手 (demand) vs 有多少活 (ready_steps)
      # 取两者的最小值
      events_to_dispatch = Enum.take(ready_steps, state.demand)
      count = length(events_to_dispatch)

      # 3. 如果有活派
      if count > 0 do
        # 3.1 标记这些步骤正在运行 (防止重复派发)
        # 注意：这里我们只取 index 列表来标记
        step_indices = Enum.map(events_to_dispatch, fn {_, idx} -> idx end)
        new_ctx = Scheduler.mark_running(state.ctx, step_indices)

        # 3.2 准备发给工人的数据
        # GenStage 要求返回 {:noreply, [事件列表], 新状态}
        # 事件就是：{具体的步骤结构, 步骤的编号, 输入参数, 全局配置}
        events =
           Enum.map(events_to_dispatch, fn {step, idx} ->
             {step, idx, new_ctx.params, new_ctx.recipe.opts}
           end)

        {:noreply, events, %{state | ctx: new_ctx, demand: state.demand - count}}
      else
        # 没活干，或者没人手，啥也不发
        {:noreply, [], state}
      end
    end
  end

  # ===========================================================================

  # 3. 工人 (Consumer)
  # 纯粹的打工仔，不仅干活，干完还要发消息告诉调度员。
  defmodule Worker do
    use GenStage

    def start_link(producer_pid, id) do
      GenStage.start_link(__MODULE__, {producer_pid, id})
    end

    @impl true
    def init({producer_pid, id}) do
      # 这里的 :subscribe_to 就像是工人去排队
      # max_demand: 1 表示：我一次只领 1 个活，干完再领
      {:consumer, %{producer: producer_pid, id: id}, subscribe_to: [{producer_pid, max_demand: 1}]}
    end

    @impl true
    def handle_events(events, _from, state) do
      # events 是一个列表，但因为 max_demand 是 1，通常里面只有 1 个任务
      Enum.each(events, fn {step, idx, params, opts} ->
        # 1. 真正的干活 (调用 Runner)
        result = Orchid.Runner.run(step, params, opts)

        # 2. 汇报结果 (Cast 给调度员)
        # 注意：这里我们用 GenServer.cast 发回给 Producer
        GenStage.cast(state.producer, {:step_finished, idx, result})
      end)

      # GenStage 规范：consumer 不产生事件，所以返回空列表
      {:noreply, [], state}
    end
  end
end
