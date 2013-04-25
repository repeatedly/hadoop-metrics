require 'hadoop_metrics/api'

module HadoopMetrics
  class TaskTracker
    include API

    def initialize(host, port, opts = {})
      @jmx_endpoint = URI("http://#{host}:#{port}/jmx?qry=hadoop:service=TaskTracker,name=TaskTrackerInfo")
      @metrics_endpoint = URI("http://#{host}:#{port}/metrics?format=json")
      @json_value_fields = %W(TasksInfoJson)
      @snake_case = opts[:snake_case] || true
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
