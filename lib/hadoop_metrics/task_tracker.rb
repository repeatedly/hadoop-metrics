require 'hadoop_metrics/api'

module HadoopMetrics
  class TaskTracker
    include API

    JSON_FILED_VALUES = %W(TasksInfoJson)

    def info
      via_jmx('hadoop:service=TaskTracker,name=TaskTrackerInfo', JSON_FILED_VALUES).first
    end

    def shuffle_output(column = 'sessionId')
      column = HadoopMetrics.to_snake_case(column) if @snake_case
      group_by('mapred', 'shuffleOutput', column)
    end

    def mapred(column = 'sessionId')
      column = HadoopMetrics.to_snake_case(column) if @snake_case
      group_by('mapred', 'tasktracker', column)
    end
  end
end
