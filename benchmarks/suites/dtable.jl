@everywhere using CSV, Arrow, Random, OnlineStats, Dates

# n = tryparse(Int, ARGS[1])
# max_chunksize = tryparse(Int, ARGS[2])
# unique_values = tryparse(Int32, ARGS[3])
# ncolumns = tryparse(Int, ARGS[4])

function dtable_suite(ctx; method, accels)
    @assert method == "dagger" "DTable suite does not support non-Dagger execution"
    @assert isempty(accels) "DTable suite does not support acceleration"

    n = Int(2e7)
    max_chunksize = Int(1e6)
    unique_values = Int(1e3)
    ncolumns = 4
    nchunks = (n+max_chunksize-1) ÷ max_chunksize

    genchunk = (rng, nchunks) -> (;[Symbol("a$i") => rand(rng, Int32(1):Int32(unique_values), n÷nchunks) for i in 1:ncolumns]...)

    suite = BenchmarkGroup()

    suite["DTable in-process generation"] = @benchmarkable begin
        DTable([Dagger.spawn($genchunk, MersenneTwister(1111+i), nchunks) for i in 1:nchunks], NamedTuple)
    end setup=begin
        nchunks = $nchunks
    end teardown=begin
        @everywhere GC.gc()
    end

    suite["DTable single CSV chunked reading"] = @benchmarkable begin
        @info "Loading CSV.Chunks -> DTable"
        c = CSV.Chunks(
            joinpath(path, "datapart_1.csv"),
            ntasks=(($n+$max_chunksize-1) ÷ $max_chunksize),
            types = Int32
        )
        DTable(c)
    end setup=begin
        @info "Writing CSV data"
        path = mktempdir()
        nchunks = 1 #overwrite nchunks to create one big file
        for i = 1:nchunks
            CSV.write(joinpath(path, "datapart_"*i*".csv"), $genchunk(MersenneTwister(1111+i), nchunks))
        end
    end teardown=begin
        rm(path; recursive=true)
        @everywhere GC.gc()
    end


    suite["DTable multiple CSV reading"] = @benchmarkable begin
        @info "Loading CSV -> DTable"
        DTable(x-> CSV.read(x, NamedTuple, types=Int32), readdir(path, join=true))
    end setup=begin
        @info "Writing CSV data"
        path = mktempdir()
        nchunks = ($n+$max_chunksize-1) ÷ $max_chunksize
        for i = 1:nchunks
            CSV.write(joinpath(path, "datapart_"*i*".csv"), $genchunk(MersenneTwister(1111+i), nchunks))
        end
    end teardown=begin
        rm(path; recursive=true)
        @everywhere GC.gc()
    end

    suite["DTable multiple Arrow reading"] = @benchmarkable begin
        @info "Loading Arrow -> DTable"
        DTable(Arrow.Table, readdir(path, join=true))
    end setup=begin
        @info "Writing Arrow data"
        path = mktempdir()
        nchunks = ($n+$max_chunksize-1) ÷ $max_chunksize
        for i = 1:nchunks
            Arrow.write(joinpath(path, "datapart_"*i*".arrow"), $genchunk(MersenneTwister(1111+i), nchunks))
        end
    end teardown=begin
        rm(path; recursive=true)
        @everywhere GC.gc()
    end

    suite["DTable multiple Arrow reading"] = @benchmarkable begin
        @info "Joining DTables"
        dd = Dagger.innerjoin(d_left, d_right, on=:a1, r_unique=true)
        wait.(dd.chunks)
    end setup=begin
        @info "Generating DTable"
        nchunks = $nchunks
        d_left = DTable([Dagger.spawn($genchunk, MersenneTwister(1111+i), nchunks) for i in 1:nchunks], NamedTuple)
        d_right = DTable((a1=Int32.(1:$unique_values), a5=.-Int32.(1:$unique_values)), Int($unique_values))
    end teardown=begin
        @everywhere GC.gc()
    end

    suite
end
