require 'net/http'
require 'json'

module HadoopMetrics
  module API
    def jmx
      jmx_json = HadoopMetrics.get_response(@jmx_endpoint)['beans'].first
      @json_value_fields.each { |f|
        jmx_json[f] = JSON.parse(jmx_json[f])
      }
      if @snake_case
        jmx_json = HadoopMetrics.snake_cased(jmx_json)
      end

      jmx_json
    end

    def metrics
      HadoopMetrics.get_response(@metrics_endpoint)
    end

    private

    def group_by(category, target, column)
      categories = metrics[category]
      return nil if categories.nil?

      targets = categories[target]
      return nil if targets.nil?

      targets.map { |target|
        HadoopMetrics.merge_data(target, @snake_case)
      }.group_by { |target|
        target[column]
      }
    end

    def method_missing(method, *args)
      category, target = method.to_s.split('_', 2)
      group_by(category, target, *args)
    end
  end

  def self.get_response(endpoint)
    response = Net::HTTP.get_response(endpoint)
    if response.code.to_i == 200
      JSON.parse(response.body)
    else
      raise "Failed to get a response: code = #{response.code}, body = #{response.body}"
    end
  end

  def self.merge_data(data, snake_case)
    f = data.first
    f.merge!(data.last)
    snake_case ? snake_cased(f) : f
  end

  def self.to_snake_case(name)
    name[0] = name[0].chr.downcase
    name.gsub(/[A-Z]/) { |n| "_#{n.downcase}" }
  end

  def self.snake_cased(json)
    snake_cased = {}
    json.each_pair { |k, v|
      snake_cased[HadoopMetrics.to_snake_case(k.dup)] = json[k]
    }
    snake_cased
  end
end
