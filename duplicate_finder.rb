#!/opt/ruby-1.9.2/bin/ruby

require 'digest'

# This script helps find duplicate files in the provided directory.
# It uses basic file size comparison at first to filter out truly duplicate files.
# Later the script analyses all files whose equal size match was found. It comprares
# SHA512 checksum of the files. Since files can be very large and SHA'ing them can consume
# time, the script analyses First, Middle and Last 4000 bytes to get the near-complete hash.
# The script also output more than one duplicate file found. The result is as below
# ./duplicate_finder.rb /home/asuryawanshi
# /home/asuryawanshi/test/META-INF/LICENSE
# /home/asuryawanshi/test/META-INF/LICENSE.txt
# /home/asuryawanshi/test/META-INF/LICENSE_1.txt
# The files are sorted by mtime, hence the first file is the oldest hence the original one
# and next two are the duplicates.
def find_duplicate_files(starting_directory)
  # We store files with matching sizes in a hash, so that only a match in size will be further compared for SHA
  files_size_hash = {}
  files_stack = [starting_directory]
  # We store the SHA in a hash as well, so all duplicagte files will have a matching SHA,
  # and can be easily processed for output.
  files_SHA_hash = {}
  # Simple array to present output.
  final_result = []

  while files_stack.length > 0
    current_path = files_stack.shift
    if File::directory? current_path
      # If current_path is Directory then we traverse it in a sorted by modified time order.
      # This is helpful for us, since we dont have to store modification time of files later.
      Dir.foreach(current_path).sort_by{|f| File.join(current_path, f)}.each do |path|
        next if path == '.' || path == '..'
        full_path = File.join(current_path, path)
        files_stack.push(full_path)
      end
    else
      file_size = File.size(current_path)
      if files_size_hash.include? file_size
        # We compute SHA of a file only if there is matching size.
        c_file_hash = sample_hash_file(current_path)
        existing_path = files_size_hash[file_size]
        e_file_hash = sample_hash_file(existing_path)
        if c_file_hash == e_file_hash
          if files_SHA_hash.include? c_file_hash
            files_SHA_hash[c_file_hash] << current_path
          else
            files_SHA_hash[c_file_hash] = [existing_path, current_path]
          end
        end
      else
        files_size_hash[file_size] = current_path
      end
    end
  end

  files_SHA_hash.each {|k,v| final_result << v}
  return final_result
end

def sample_hash_file(path)
  # Configurable size of sample. 4KB is just a good example
  num_bytes_to_read_per_sample = 4000
  total_bytes = File.size?(path).to_i
  hasher = Digest::SHA512.new
  File.open(path, 'r') do |file|
    if total_bytes < num_bytes_to_read_per_sample * 3
      hasher.update file.read()
    else
      # we try to find the offset to seek the file to, for computi9ng the SHA
      num_bytes_between_samples = (total_bytes - num_bytes_to_read_per_sample * 3) / 2
      0.upto(2) do |offset_multiplier|
        start_of_sample = offset_multiplier * (num_bytes_to_read_per_sample + num_bytes_between_samples)
        file.seek(start_of_sample)
        sample = file.read(num_bytes_to_read_per_sample)
        hasher.update sample
      end
    end
  end
  return hasher.hexdigest
end

puts find_duplicate_files(ARGV[0])
