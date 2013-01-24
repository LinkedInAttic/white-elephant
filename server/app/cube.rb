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

require 'enumerator'

class Cube
  attr_accessor :dimensions, :cube
  
  include Enumerable
  
  def initialize(dimensions)
    dimensions.each { |dim| raise "expected symbol for dimension" unless dim.is_a? Symbol }
    @dimensions = dimensions
    @cube = {}
  end

  def pack
    Marshal.dump([@dimensions,@cube])
  end
  
  class << self
    def unpack(packed)
      dimensions, cube_data = Marshal.load(packed)
      cube = Cube.new(dimensions)
      cube.cube = cube_data
      cube
    end

    def build(dimensions,data)
      # puts "building with dimensions: #{dimensions}"
      cube = Cube.new(dimensions)
      
      data.each do |properties,values|
        keys = dimensions.map do |dimension|
          prop = properties[dimension]
          break unless prop
          prop
        end
        
        # If any schema values missing there is nothing to count.
        next unless keys and keys.size == dimensions.size
        
        cube.aggregate!(keys,values)
      end
      
      cube
    end
  end
  
  # Stores values into the cube.  If the key already exists then values are aggregated.
  def aggregate!(keys,values)
    raise "key length does not match dimension length" unless keys.size == @dimensions.size

    subcube = keys.inject(@cube) do |subcube,key|
      subcube[key] ||= {}
    end
    
    values.each do |key,value|
      if subcube.key?(key)
        if value && subcube[key]
          subcube[key] = subcube[key] + value
        end
      else
        subcube[key] = value
      end
    end
  end
  
  # Stores values into the cube.  Raises an error if the key already exists.
  def put!(keys,values)
    raise "key length does not match dimension length" unless keys.size == @dimensions.size
    
    subcube = keys.inject(@cube) do |subcube,key|
      subcube[key] ||= {}
    end
    
    values.each do |key,value|
      if subcube.key?(key)
        raise "#{key} already exists for keys #{keys.inspect}"
      else
        subcube[key] = value
      end
    end
  end
  
  # filters values within a particular dimension
  def filter_on(dimension,&b)
    dimension_index = @dimensions.index(dimension)
    raise "dimension #{dimension} not found" unless dimension_index && dimension_index >= 0
    
    cube = Cube.new(@dimensions)
    self.each do |keys,values|
      if b.call(keys[dimension_index])
        cube.aggregate!(keys,values)
      end
    end
    cube
  end
  
  # aggregates over a particular dimension
  def aggregate_on(dimension,&b)
    dimension_index = @dimensions.index(dimension)
    raise "dimension #{dimension} not found" unless dimension_index >= 0
    
    cube = Cube.new(@dimensions)
    self.each do |keys,values|
      keys[dimension_index] = b.call(keys[dimension_index])
      cube.aggregate!(keys,values)
    end
    cube
  end

  # drops the dimension and aggregates the result over the new dimensions
  def collapse_on(dimension)
    dimension_index = @dimensions.index(dimension)
    raise "dimension #{dimension} not found" unless dimension_index >= 0
    
    new_dimensions = @dimensions.slice(0..-1)
    new_dimensions.delete_at(dimension_index)

    cube = Cube.new(new_dimensions)
    self.each do |keys,values|
      keys.delete_at(dimension_index)
      cube.aggregate!(keys,values)
    end
    cube
  end
  
  def has_slice?(key)
    @cube[key]
  end
  
  def slice(key)
    if has_slice? key
      cube = Cube.new(@dimensions.slice(1..-1))
      # TODO we're reusing the subsube, which could be bad.  Simpler method may be to chain ops together and curry the slice.
      cube.cube = @cube[key]
      cube
    else
      nil
    end
  end
  
  # enumerates all keys for a particular dimension, but does not eliminate duplicates
  def keys_for(dimension)
    SingleKeyEnumerable.new(self,dimension)
  end
  
  def keys
    KeyEnumerable.new(self)
  end
  
  def values
    ValueEnumerable.new(self)
  end
  
  def each(&b)
    KeyValueEnumerable.new(self,:both).each(&b)
  end

  private

  # Enumerates all keys and values.  
  class KeyValueEnumerable
    include Enumerable
    
    def initialize(cube,which=:both)
      @cube = cube
      @which = which
    end
    
    def each(&b)
      level = @cube.dimensions.size-1
      each_for_level([],@cube.cube,level,&b)
    end
    
    private
    
    def each_for_level(keys,subcube,level,&b)
      if level == 0
        subcube.each do |key,subcube|
          keys << key
          case(@which)
          when :both
            b.call([keys.clone,subcube])
          when :keys
            b.call(keys.clone)
          when :values
            b.call(subcube)
          end
          keys.slice!(-1)
        end
      else
        subcube.each do |key,subcube|
          keys << key
          each_for_level(keys,subcube,level-1,&b)
          keys.slice!(-1)
        end
      end
    end
  end

  # Enumerates keys for a particular dimension.  It does not eliminate duplicates.
  # It literally walks the tree and returns all values at the level for the dimension.
  class SingleKeyEnumerable
    include Enumerable
    
    def initialize(cube,dimension)
      raise "#{dimension} not found in cube dimensions #{cube.dimensions}" unless cube.dimensions.index(dimension)
      @cube = cube
      @dimension = dimension
    end
    
    def each(&b)
      level = @cube.dimensions.index(@dimension)
      each_for_level(@cube.cube,level,&b)
    end
    
    private
    
    def each_for_level(subcube,level,&b)
      if level == 0
        subcube.each do |key,subcube|
          b.call(key)
        end
      else
        subcube.each do |key,subcube|
          each_for_level(subcube,level-1,&b)
        end
      end
    end
  end

  class KeyEnumerable < KeyValueEnumerable
    def initialize(cube)
      super(cube,:keys)
    end
  end
  
  class ValueEnumerable < KeyValueEnumerable
    def initialize(cube)
      super(cube,:values)
    end
  end
end
