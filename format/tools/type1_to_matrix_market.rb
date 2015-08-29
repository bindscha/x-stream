#!/usr/bin/env ruby

if (ARGV.size != 2)
  puts "Usage: #{$0} input_file output_file"
  exit
end
input_file = ARGV[0]
output_file = ARGV[1]

File.open(output_file, "w") do |outf|
  # Header
  ini_contents = File.read("#{input_file}.ini")
  vertices = ini_contents.scan(/vertices=(.*)/).to_s.to_i
  edges =    ini_contents.scan(/edges=(.*)/).to_s.to_i
  outf.puts "%%MatrixMarket matrix coordinate real general"
  outf.puts "#{vertices} #{vertices} #{edges}"

  File.open(input_file, "r") do |inf|
    edge_struct = inf.read(12)
    while (edge_struct != nil)
      from = edge_struct[0..3].unpack("L").first + 1
      to = edge_struct[4..7].unpack("L").first + 1
      value = edge_struct[8..11].unpack("f").first
      outf.puts "#{from} #{to} #{value}"
      edge_struct = inf.read(12)
    end
  end
end

