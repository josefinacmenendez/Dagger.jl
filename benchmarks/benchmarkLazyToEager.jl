using BenchmarkTools, Dagger
#=
This script times several operations on DArrays 
=#
println("Running with $(Threads.nthreads()) threads")
A = compute(rand(Blocks(16,16),64,64))
a = compute(rand(Blocks(16),64))
println( "A = compute(rand(Blocks(16,16),64,64): "); @btime A = compute(rand(Blocks(16,16),64,64))
println( "a = compute(rand(Blocks(16),64): "); @btime a = compute(rand(Blocks(16),64))
println( "A*A: "); @btime A*A 
println( "A*A': "); @btime A*A'
#println( "sort(a): "); @btime sort(a) 
println( "A .* A .+ 42.0: "); @btime A .* A .+ 42.0
println( "map(x->x+1.0,A): "); @btime map(x->x+1.0,A) 
println( "reduce(+,A): "); @btime reduce(+,A) 
println( "A[1,:]: "); @btime A[1,:] 
println( "vcat(A,A): "); @btime vcat(A,A) 


