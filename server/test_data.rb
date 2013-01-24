# Copyright 2012 LinkedIn, Inc

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'fileutils'

Dir[File.join("lib/jars","*.jar")].each do |lib|
  require lib
end

core_jar = File.expand_path("../hadoop/dist/white-elephant-core-0.0.1.jar")
unless File.exists? core_jar
  raise "#{core_jar} not found"
end
require core_jar

java_import com.linkedin.whiteelephant.analysis.AttemptStats
java_import com.linkedin.whiteelephant.analysis.AttemptStatsKey
java_import com.linkedin.whiteelephant.analysis.AttemptStatsValue
java_import com.linkedin.whiteelephant.analysis.TaskStatus
java_import com.linkedin.whiteelephant.analysis.TimeUnit
java_import com.linkedin.whiteelephant.analysis.TaskType
java_import org.apache.avro.Schema
java_import org.apache.avro.file.DataFileWriter
java_import org.apache.avro.generic.GenericDatumWriter
java_import org.apache.avro.generic.GenericData

def create_schema()
  key_schema = AttemptStatsKey.new.schema
  value_schema = AttemptStatsValue.new.schema
  key_field = Schema::Field.new("key",key_schema,"the key",nil)
  value_field = Schema::Field.new("value",value_schema,"the value",nil)
  schema = Schema.createRecord("KeyValuePair","a key/value pair", "com.linkedin.whiteelephant",false)
  schema.set_fields([key_field,value_field])
  schema
end

def create_writer(schema)
  writer = DataFileWriter.new(GenericDatumWriter.new(schema))

  usage_dir = "data/usage"
  usage_file = File.join(usage_dir,"test.avro")

  if File.directory? usage_dir
    FileUtils.rm_rf(usage_dir)
  end

  FileUtils.mkdir_p(usage_dir)

  writer.create(schema,Java::java.io.File.new(usage_file))

  writer
end

def get_times(duration_in_days)
  end_time = Time.now.getutc.to_i*1000
  start_time = end_time - duration_in_days*24*3600*1000

  step_size_in_ms = 3600*1000

  # round to nearest hour, then convert to seconds
  start_time = ((start_time/(step_size_in_ms.to_f)).floor*step_size_in_ms).to_i
  end_time = ((end_time/(step_size_in_ms.to_f)).floor*step_size_in_ms).to_i

  times = []
  (start_time..end_time).step(step_size_in_ms).each do |time|
    times << time
  end
  times
end

def positive_gaussian(mean, stddev)
  theta = 2 * Math::PI * rand
  rho = Math.sqrt(-2 * Math.log(1 - rand))
  scale = stddev * rho
  x = mean + scale * Math.cos(theta)
  return [x,0.0].max
end

schema = create_schema
writer = create_writer(schema)

clusters = %w|dev-cluster prod-cluster|
users = ('A'..'G').map { |l| "User-#{l}"}.to_a
times = get_times(120) # about 4 months

clusters.each do |cluster|
  users.each do |user|
    times.each do |time|
      status_values = [TaskStatus::SUCCESS, TaskStatus::KILLED, TaskStatus::FAILED]
      type_values = [TaskType::REDUCE, TaskType::MAP]
      excess_values = [true, false]

      status_values.each do |status|

        status_tasks = case status
        when TaskStatus::SUCCESS
          positive_gaussian(100.0,50.0).to_i
        when TaskStatus::KILLED
          positive_gaussian(10.0,5.0).to_i
        when TaskStatus::FAILED
          positive_gaussian(1.0,10.0).to_i
        else
          raise "Unexpected: #{status}"      
        end

        # just make elapsed minutes a random multiple of number of tasks
        status_elapsed_minutes = status_tasks * [[1.0,positive_gaussian(30.0,15.0)].max,120.0].min

        type_division = [positive_gaussian(0.75,0.25),1.0].min

        type_values.each do |type|

          case type
          when TaskType::REDUCE
            type_elapsed_minutes = (status_elapsed_minutes * type_division)
            type_tasks = (status_tasks * type_division).to_i
            type_reduce_shuffle_bytes = positive_gaussian(1000,100).to_i
          when TaskType::MAP
            type_elapsed_minutes = (status_elapsed_minutes * (1.0 - type_division))
            type_tasks = (status_tasks * (1.0 - type_division)).to_i
            type_reduce_shuffle_bytes = 0
          else
            raise "Unexpected: #{type}"      
          end

          excess_division = [positive_gaussian(0.95,0.05),1.0].min

          excess_values.each do |excess|

            case excess
            when false
              elapsed_minutes = (type_elapsed_minutes * excess_division)
              tasks = (type_tasks * excess_division).to_i
              reduce_shuffle_bytes = (type_reduce_shuffle_bytes * excess_division).to_i
            when true
              elapsed_minutes = (type_elapsed_minutes * (1.0 - excess_division))
              tasks = (type_tasks * (1.0 - excess_division)).to_i
              reduce_shuffle_bytes = (type_reduce_shuffle_bytes * (1.0 - excess_division)).to_i
            else
              raise "Unexpected: #{excess}"    
            end

            record = GenericData::Record.new(schema)

            key = AttemptStatsKey.new
            key.set_cluster cluster
            key.set_user user
            key.set_type type
            key.set_unit TimeUnit::HOURS
            key.set_excess excess
            key.set_status status
            key.set_time time

            cpu_minutes = elapsed_minutes * positive_gaussian(1.0,0.05)

            value = AttemptStatsValue.new
            value.set_started tasks
            value.set_finished tasks
            value.set_elapsed_minutes elapsed_minutes
            value.set_cpu_minutes cpu_minutes
            value.set_spilled_records positive_gaussian(1000,100).to_i
            value.set_reduce_shuffle_bytes reduce_shuffle_bytes

            record.put("key",key)
            record.put("value",value)

            writer.append(record)
          end
        end
      end
      
    end 
  end
end

writer.close
