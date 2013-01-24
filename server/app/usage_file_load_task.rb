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

include_class java.util.concurrent.Callable

class UsageFileLoadTask

  include Callable

  def initialize(file_name,modified_time,local_file_name,delete=false)
    @file_name = file_name
    @modified_time = modified_time
    @local_file_name = local_file_name
    @delete = delete
  end

  class << self

    # create thread local connection so we don't need to keep recreating one per task
    @@conn = Class.new(Java::java.lang.ThreadLocal) do 
      def initialValue
        # puts "Creating connection!"
        Java::java.sql.DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "")
      end
    end.new

    def conn
      @@conn
    end
  end

  # implement Callable interface
  def call

    connection = UsageFileLoadTask.conn.get

    # record that the file has been processed using the original file name
    # TODO fix the assumption that the rest of the code succeeds
    prepared_statement = connection.prepareStatement("INSERT INTO LoadedFiles (fileName,modified) VALUES (?,?)")
    prepared_statement.set_string(1,@file_name)
    prepared_statement.set_timestamp(2,Java::java.sql.Timestamp.new(@modified_time.to_i*1000))
    prepared_statement.execute_update
    prepared_statement.close

    prepared_statement = connection.prepareStatement("SELECT id FROM LoadedFiles WHERE fileName=?")
    prepared_statement.set_string(1,@file_name)
    result = prepared_statement.execute_query
    file_id = if result.next
      result.get_int(1)
    else
      raise "Did not find file id"
    end

    prep_statement = connection.prepareStatement("INSERT INTO Usage VALUES (" + 13.times.map{"?"}.join(",") + ")")

    prep_statement2 = connection.prepareStatement("SELECT * FROM Usage WHERE userName=? AND time=? AND cluster=? AND excess=? AND type=? AND status=?")

    last_time = Time.now
    
    max_batch_size = 1000

    batch_size = 0
    records_read = 0

    key_fields = %w|user time unit cluster excess type status|.map { |k| [k,k.to_sym] }
    value_fields = %w|started finished elapsedMinutes cpuMinutes reduceShuffleBytes|.map { |k| [k,k.to_sym] }

    record = nil
    
    if File.file? @local_file_name
      loader = Java::org.apache.avro.file.DataFileReader.new(Java::java.io.File.new(@local_file_name),Java::org.apache.avro.generic.GenericDatumReader.new)        
      while loader.has_next do 
        record = loader.next(record)

        key_record = record.get("key")
        value_record = record.get("value")

        key_fields.each do |key_field|
          value = convert_avro_value(key_record.get(key_field[0]))
          case key_field[1]
          when :user
            prep_statement.set_string(1,value)
          when :time
            prep_statement.set_timestamp(2,Java::java.sql.Timestamp.new(value))
            prep_statement.set_long(13,value)
          when :cluster
            prep_statement.set_string(3,value)
          when :excess
            prep_statement.set_boolean(4,value)
          when :type
            prep_statement.set_string(5,value)
          when :status
            prep_statement.set_string(6,value)
          end 
        end

        value_fields.each do |value_field|
          value = convert_avro_value(value_record.get(value_field[0]))
          case value_field[1]
          when :started
            prep_statement.set_int(7,value)
          when :finished
            prep_statement.set_int(8,value)
          when :elapsedMinutes
            prep_statement.set_double(9,value)
          when :cpuMinutes
            prep_statement.set_double(10,value)
          when :reduceShuffleBytes
            prep_statement.set_long(11,value)
          end 
        end

        prep_statement.set_int(12,file_id)

        prep_statement.add_batch

        batch_size += 1
        records_read += 1

        if batch_size >= max_batch_size
          prep_statement.execute_batch
          batch_size = 0
        end
      end

      if @delete
        File.delete(@local_file_name)
      end
    else
      puts "Not a file: #{@local_file_name}"
    end

    if batch_size > 0
      prep_statement.execute_batch
      batch_size = 0
    end

    puts "Finished loading #{records_read} records from #{@file_name}"

    prep_statement.close

  rescue Exception => ex
    puts "Failed loading file #{@file_name}: #{ex}"
    puts ex.backtrace.join("\n")

    # Remove all data inserted into database for this file so there isn't any partial data.
    # Better to have a noticeable gap in data than a subset.
    puts "Cleaning up data for file #{@file_name} with ID #{file_id}"
    prepared_statement = connection.prepareStatement("DELETE FROM LoadedFiles WHERE id=?")
    prepared_statement.set_int(1,file_id)
    prepared_statement.execute_update
    prepared_statement.close

    raise ex
  end

  def convert_avro_value(value)
    case value

    when Java::OrgApacheAvroUtil::Utf8
      value.to_s

    when Java::OrgApacheAvroGeneric::GenericData::EnumSymbol
      value.to_s

    when Fixnum, Float, FalseClass, TrueClass, NilClass
      value

    else
      raise "Unexpected type: #{value.class}"
    end
  end
end