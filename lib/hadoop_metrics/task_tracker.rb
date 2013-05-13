require "hadoop_metrics/api"

module HadoopMetrics
  class TaskTracker
    include API

    JSON_FILED_VALUES = %W(TasksInfoJson)

    def info
      query_jmx('hadoop:service=TaskTracker,name=TaskTrackerInfo', JSON_FILED_VALUES).first
    end

    def shuffle_output(opts = {})
      column = get_column(opts)
      column = HadoopMetrics.to_snake_case(column) if @snake_case
      group_by('mapred', 'shuffleOutput', column, get_force(opts))
    end

    def mapred(opts = {})
      column = get_column(opts)
      column = HadoopMetrics.to_snake_case(column) if @snake_case
      group_by('mapred', 'tasktracker', column, get_force(opts))
    end

    private

    def get_column(opts)
      opts[:column] || 'sessionId'
    end
  end
end
