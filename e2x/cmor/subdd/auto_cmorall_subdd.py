# Program name -  "auto_cmorall_subdd"
# Purpose - To automate the invoking of cmor at subdaily scales for all types of standard runs
# $1 Master cmor script (usually master_cmor3.ksh)
# $2 run name
# $3 year start
# $4 year end

# Imports
import sys
import os
import subprocess
import glob

# Export path
def set_path():
    """
    Set PATH environment variable for subprocesses.
    """
    username = subprocess.check_output("whoami", shell=True, text=True).strip()
    path1 = f'/discover/nobackup/{username}/CMOR3.3.2'
    path2 = f'/discover/nobackup/{username}/proc_cmip6'

    # Modify PATH environment variable
    os.environ['PATH'] = f'{path1}:{path2}:{os.environ["PATH"]}'

# Ensure sufficient CLA
def check_args():
    '''
    Must pass CLA or script exits gracefully
    '''
    # Check if the correct number of command line arguments are provided
    if len(sys.argv) < 4:
        print("Usage: python auto_cmorall_subdd.py runname startyear endyear compress")
        print('Optional 5th argument "compress" to turn on compression')
        sys.exit(1)

# dynamic function, any res ## check for accuracy
def cmorit(master, runname, startyear, endyear, tres, num_pday, tres_ychunk, YN_6hrL):
    '''
    tres output
    Usually done in tres_ychunk year increments (e.g. 1850 - 1874)

    master = master cmor script
    runname = internal simulation name
    starty = first year being cmored
    endy = last year being cmored
    tres = time resolution (str) (e.g. 3hr)
    num_pday = number of times per day (e.g. 3hr = 8 num_pday)
    tres_ychunk = time resolution max years proc at once (int)
    YN_6hrL = Binary Y/N for whether processing 6hrL data (different # command line args)
    '''

    # Extract years info from start/end year to get processing chunks
    ## Need to process data in chunks due to how much data the fortran programs are able to ingest at once
    ### Dependent upon number of bytes in a record (e.g. daily data with pressure levels > 2d daily data, so less years can be processed at once for pressure level data)
    numyears = endyear + 1 - startyear
    numblocks = int(numyears/tres_ychunk // 1) # tres_ychunk yr blocks for tres data

    # If the time resolution chunk exceeds the total number of years passed, then just process the entire period passed
    if numblocks == 0:
        y1 = startyear
        y2 = endyear
        num_data_points = (endyear + 1 - startyear)*num_pday*365 # numyears*timesinday*daysinyear (number data points per chunk)
        numblocks = 1 # reset numblocks to one, so only one iteration is processed (full period)
    # Otherwise process in blocks of N year periods dependent upon # bytes per record, get number of data points per chunk
    else:
        y1 = startyear
        y2 = (startyear + tres_ychunk - 1)
        num_data_points = tres_ychunk*num_pday*365 # numyears*timesinday*daysinyear

    # Run CMOR script for time blocks
    for i in range(numblocks):
        # Select syntax based on whether processing 6hrL or not
        if YN_6hrL.lower() == 'y':
            doit_cmor = f'{master} {runname} {y2} {startyear}'
        else:
            doit_cmor = f'{master} {runname} {tres} {num_data_points} {y1} {y2} {startyear}'
        subprocess.call(doit_cmor, shell=True)
        y1 = y2 + 1
        y2 = y1 + tres_ychunk - 1

    # Do the remainder (if any)
    if y1 < endyear:
        y2 = endyear # Since less than tres_ychunk years here, cap at end year
        num_data_points = (y2 + 1 - y1)*num_pday*365 # numyears*timesinday*daysinyear
        if YN_6hrL.lower() == 'y':
            doit_cmor = f'{master} {runname} {y2} {startyear}'
        else:
            doit_cmor = f'{master} {runname} {tres} {num_data_points} {y1} {y2} {startyear}'
        subprocess.call(doit_cmor, shell=True)

    print(f'~~~~~~ Completed:  {doit_cmor} ~~~~~')


def get_output_dirs():
    '''
    Find the output directory structure using the 
    most recent file by time stamp in the CMIP6 
    directory.
    This will be used for compression and also
    retrieving the 6hrL files which are outputted
    to a different directory (../CMOR3_6hrL).
    '''

    # Get a list of all files matching the pattern
    CMIP6_files = glob.glob("CMIP6/*/*/*/*/*/*/*/*/*/*_gn_*.nc")
    
    # Check if files were found in glob
    if len(CMIP6_files) > 0:
        # Get the latest file based on modification time (to pick out the latest run)
        latest_file = max(CMIP6_files, key=os.path.getmtime)
        file_path_array = latest_file.split("/") # array of all components of the file path

    else: 
        print('Attemp to locate output directory structure failed')
        print('No CMIP6 files found in assumed location (CMOR3.3.2/CMIP6). Either there are no CMORized outputs or something else is wrong')
        print('Please check CMOR3.3.2 to confirm')
        print('Exiting...')
        sys.exit(0)

    return file_path_array


def retrieve_6hrLev(file_path_array):
    '''
    Currently, the subdailly CMOR process is set up to CMORize and export 6hrLev
    data to a separate directory from the rest of the outputs. 
    This directory is in ../CMOR3_6hrL/
    The reasoning behind this programming choice is being investigated, but 
    according to tests, the 6hrLev data does not get processed correctly when try to 
    CMORize and export within CMOR3.3.2.
    The band aid solution to this is to just copy the 6hrLev data over after
    CMORization. 
    This is the purpose of this fucntion.
    '''

    # Get normal CMIP6 data output path up to the variant
    CMIP6up2variant = f'{file_path_array[0]}/{file_path_array[-10]}/{file_path_array[-9]}/{file_path_array[-8]}/{file_path_array[-7]}/{file_path_array[-6]}'
    # Get the 6hrLev data from the relevant directory 
    path2data = f'../CMOR3_6hrL/{CMIP6up2variant}/6hrLev'
    # Copy the data to the normal CMIP6 filesystem, where it can happily coexist with its CMIP data counterparts
    if os.path.exists(path2data):
        mv_6hrLev = f'mv {path2data} {CMIP6up2variant}/'
        subprocess.call(mv_6hrLev, shell=True)
    else:
        print(f'No 6hrLev output found - {path2data}. Check your file system')
        print(f'Proceeding without copying 6hrLev folder...')


# Option compression add-on
def compress_all(compress_binary, file_path_array):
    '''
    Option to compress all the NetCDF files 
    produced by subdaily CMOR process.
    Will use the most recent file by timestamp
    to determine the run that needs to be compressed.
    '''

    if compress_binary == 1:
        print("Compressing NetCDF files with level 1 compression...")

        for ncfile in glob.glob(f"{file_path_array[0]}/{file_path_array[-10]}/{file_path_array[-9]}/{file_path_array[-8]}/{file_path_array[-7]}/{file_path_array[-6]}/*/*/*/*/*_gn_*nc"):
            compressit = f"ncks -4 -L 1 -h -O {ncfile} {ncfile}"
            subprocess.call(compressit, shell=True)

        print('File compression finished! :)')

    else:
        print("No files have been compressed because no files were found!")


# Set up execution of proc
def run():
    check_args()
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
    
    # Export path
    set_path()

    # Print vars running
    print("\n!!!!!!! RUNING CMOR WITH PARAMETERS BELOW !!!!!!!")
    print("| Master:", master)
    print("| Runname:", runname)
    print("| Startyear:", startyear)
    print("| Endyear:", endyear)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
    
    ## CMORization
    # 3hr
    cmorit(master, runname, startyear, endyear, "3hr", 8, 5, "n")
    # 6hr2d
    cmorit(master, runname, startyear, endyear, "6hr2d", 4, 25, "n")
    # 6hrP
    cmorit(master, runname, startyear, endyear, "6hrP", 4, 1, "n")
    # day2d
    cmorit(master, runname, startyear, endyear, "day2d", 1, 50, "n")
    # day3d
    cmorit(master, runname, startyear, endyear, "day3d", 1, 5, "n")
    # 6hrL
    cmorit(master_6hrL, runname, startyear, endyear, "6hrL", 4, 1, "y")
    
    ## Clean up and compression
    # Get the output directory structure
    file_path_info = get_output_dirs()
    # Move 6hrL data to the folder with the rest of the data
    retrieve_6hrLev(file_path_info)
    # Compress the data if argument passed
    compress_all(do_compress, file_path_info)

# Run it
run()

print("!!!!!!! SUBDAILiY CMORizing COMPLETE !!!!!!!")
