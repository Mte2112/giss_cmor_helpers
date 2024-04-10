from cmorpytools.subdd.cmorall import All as sdc # sdc = SubDd Cmor 
from cmorpytools.subdd.corrections import RSDS as crsds # crsds = Correct RSDSdiff
import glob
import sys

# Set up execution of proc
def run():
    sdc.check_args()
    # Extract mandatory command line arguments
    master = "master_cmor3.ksh"
    runname = sys.argv[1]
    startyear = sys.argv[2]
    endyear = sys.argv[3]

    # Extract optional compression argument
    if (len(sys.argv) > 4):
        if (sys.argv[4].lower() == "compress"):
            do_compress = 1 # compression binary
        else:
            print("Too many command line aguments passed OR compression argument not understandable")
            print("If you would like compression, enter 'compress' as the 6th argument")
            print("Exiting...")
            sys.exit(0)
    else:
        do_compress = 0 # compression binary
        print("No compression argument passed. NOT compressing data")
    
    # Set variable for 6hrL master
    master_6hrL = "master_cmor3_6hrL.ksh"

    # Convert startyear and endyear to integers
    try:
        startyear = int(startyear)
        endyear = int(endyear)
    except ValueError:
        print("Error: startyear and endyear must be integers")
        sys.exit(1)
    
    # Export path and get cmor3.3.2, proc dirs of discover user
    cmordir, procdir = sdc.set_path()

    # Print vars running
    print("\n!!!!!!! RUNING CMOR WITH PARAMETERS BELOW !!!!!!!")
    print("| Master:", master)
    print("| Runname:", runname)
    print("| Startyear:", startyear)
    print("| Endyear:", endyear)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
    
    ## CMORization
    # 3hr
    sdc.cmorit(master, runname, startyear, endyear, "3hr", 8, 5, "n")
    # 6hr2d
    sdc.cmorit(master, runname, startyear, endyear, "6hr2d", 4, 25, "n")
    # 6hrP
    sdc.cmorit(master, runname, startyear, endyear, "6hrP", 4, 1, "n")
    # day2d
    sdc.cmorit(master, runname, startyear, endyear, "day2d", 1, 50, "n")
    # day3d
    sdc.cmorit(master, runname, startyear, endyear, "day3d", 1, 5, "n")
    # 6hrL
    sdc.cmorit(master_6hrL, runname, startyear, endyear, "6hrL", 4, 1, "y")
    
    ## Clean up stray 6hrL directory
    # Get the output directory structure
    file_path_info = sdc.get_output_dirs()
    # Move 6hrL data to the folder with the rest of the data
    sdc.retrieve_6hrLev(file_path_info)

    ## Correct the rsdsdiff data, if relevant
    # Set path to cosz file
    cosz_f = '/discover/nobackup/projects/cmip6/e2staging/donotdelete/rsdsdiff_correction/cosz3hr.nc'

    # Glob rsdsdiff 3-hourly files
    CMIP6up2variant = f'{file_path_info[0]}/{file_path_info[-10]}/{file_path_info[-9]}/{file_path_info[-8]}/{file_path_info[-7]}/{file_path_info[-6]}'
    rsdsglob = glob.glob(f'{cmordir}/{CMIP6up2variant}/3hr/rsdsdiff/*/*/*.nc')
    
    # Check if 3hr/rsdsdiff files detected and if they are within the date range specified in arguments
    rsds_instance = crsds() # create instance of class
    rsds_instance.check_files(rsdsglob, cosz_f, cmordir, startyear, endyear)

    # Compress the data if argument passed
    sdc.compress_all(do_compress, file_path_info)

# Run it
run()
