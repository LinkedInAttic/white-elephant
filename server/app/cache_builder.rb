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

class CacheBuilder

  class << self
    def create(klass,expiry_in_seconds=nil)
      cache_loader = Class.new(Java::com.google.common.cache.CacheLoader) do

        @klass = klass

        def load(key)
          puts "Loading from #{self.class.klass.name}:\n#{key}"
          self.class.klass.new(key).call
        end

        def reload(key, oldValue)
          puts "Reloading from #{self.class.klass.name}:\n#{key}"
          # reload asynchronously
          task = Java::com.google.common.util.concurrent.ListenableFutureTask.create(self.class.klass.new(key))
          Executor.execute(task)
          task
        end

        class << self
          attr_accessor :klass
        end
      end.new

      builder = Java::com.google.common.cache.CacheBuilder.newBuilder

      if expiry_in_seconds
        builder = builder.refreshAfterWrite(expiry_in_seconds,Java::java.util.concurrent.TimeUnit::SECONDS)
      end

      builder.build(cache_loader)
    end
  end
end