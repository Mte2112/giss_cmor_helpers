import xarray as xr
import pandas as pd
from collections import Counter
import cftime
import datetime as dt
import sys
import os
import timeit
import getopt
import argparse
import uuid
import glob
import subprocess

# Auxilary funcs for CMOR corrections

class RSDS:
    """  The 3-hourly rsdsdiff diagnostic is incorrect when it 
    comes out of the CMOR process.
    The data need to be multiplied by cos(zenith) (cosz).
    The fix is to multiply the inital CMORized data
    by cosz scalar, as instructed by Max Kelley. 
    As of 20240405, the cosz data file is currently in 
    /discover/nobackup/projects/cmip6/e2staging/donotdelete/rsdsdiff_correction/cosz3hr.nc
    """ 
    
    def __init__(self):
        pass

    # Read file into xarray 
    def read_file(self, rsdsdiff_file):
        '''
        Read rsdsdiff file based on inputs in auto_cmorall_subdd.py
        '''

        # Get file names
        rsdsdiff_orig = xr.open_dataset(rsdsdiff_file)
    
        return rsdsdiff_orig

    
    # Get directory structure and file name
    def make_dir(self, rsdsdiff_file, cmordir):
        '''
        Make the temporary directory structure for rsdsdiff files
        '''
        
        ncfile = rsdsdiff_file.split('/')[-1]
        dirs = rsdsdiff_file.split(cmordir)[1].split(ncfile)[0]
        try:
            makedirs_command = f'mkdir -p {cmordir}/rsdsdiff_files/{dirs}'
            subprocess.call(makedirs_command, shell=True)
        except:
            print(f'Could not make directories {cmordir}/rsdsdiff_files/{dirs}')

    
    # Open datasets
    def open_datasets(self, cosz_f, rsdsdiff_f):
        '''
        Open rsdsdiff and cosz datasets and get data array 
        '''
        ds_arr = []
        cosz_ds = xr.open_dataset(cosz_f)
        rsdsdiff_ds = xr.open_dataset(rsdsdiff_f)
        ds_arr.append(cosz_ds)
        ds_arr.append(rsdsdiff_ds)
        
        return ds_arr
        
    
    # save original rsdsdiff file for metadata
    def get_meta(self, cosz_f, rsdsdiff_f):
        '''
        Get rsdsdiff metadata from orig file
        '''
        ds_array = [cosz_f, rsdsdiff_f]
        for dataset in ds_array:
            if 'rsdsdiff' in xr.open_dataset(dataset).data_vars:
                metads = xr.open_dataset(dataset)
        
        return metads
        

    # Align cosz coordinates
    def match_coords(self, ds):
        '''
        Make sure cosz lat are the same as rsdsdiff lat
        '''
        # If cosz lat first and last val are -90,90 then switch to -89, 89 to match CMORized data
        if ds.lat[0] == -90:
            mocklat = ds.lat.values
            mocklat[0] = -89
            mocklat[-1] = 89
            ds['lat'] = mocklat
        else: 
            print('cosz lat not -90-90')
        return ds
    
        
    # Longitude converter
    def lon360(self, ds):
        '''
        Adjust lon values to make sure they are within (0, 360) as CMOR requires
        '''
        # Create string for dataset assigning
        strds = str(ds)
        
        # Get longitude coord name
        try:
            a = ds['lon'] # if it fails, there is no coord named "lon"
            lon = 'lon'
        except:
            lon = 'longitude'
        
        # If on -180,180 grid, convert to 0-360 
        ds['_longitude_adjusted'] = xr.where(
            ds[lon] < 0,
            ds[lon] + 360,
            ds[lon])
    
        # reassign the new coords to as the main lon coords
        # and sort DataArray using new coordinate values
        locals()[strds] = (
            ds
            .swap_dims({lon: '_longitude_adjusted'})
            .sel(**{'_longitude_adjusted': sorted(ds._longitude_adjusted)})
            .drop(lon))
    
        locals()[strds] = locals()[strds].rename({'_longitude_adjusted': lon})
        
        return(locals()[strds])
        
    
    # Arrange and group the data by hour of year for matching
    def org_time_cosz(self, ds):
        '''
        Match time for cosz 
        '''
        
        da = ds.cosz2
        da['hourofyear'] = xr.DataArray(da.time.dt.strftime('%m-%d %H'), coords=da.time.coords)
        result = da.hourofyear.values
        
        return result
        
    
    def gb_hoy(self, ds):
        '''
        Groupby the hour of year
        '''
        
        da = ds.rsdsdiff
        da['hourofyear'] = xr.DataArray(da.time.dt.strftime('%m-%d %H'), coords=da.time.coords)
        result = da.groupby('hourofyear')
        
        return result
        
    
    def run(self, rsdsdiff_f, coszfile, cmordir):
        '''
        Run the 3hr/rsdsdiff correction process
        '''

        # Point to cosz file 
        cosz_f = coszfile
        cosz_f = '/discover/nobackup/projects/cmip6/e2staging/donotdelete/rsdsdiff_correction/cosz3hr.nc'

        # Get rsdsdiff xarray ds
        rsdsdiff_orig = self.read_file(rsdsdiff_f)
        
        # Open the datasets regardless of what order they are in command, even though they should be cosz, rsdsdiff
        if 'rsdsdiff' in self.open_datasets(cosz_f, rsdsdiff_f)[0].data_vars:
            rsdsdiff = self.open_datasets(cosz_f, rsdsdiff_f)[0]
        elif 'rsdsdiff' in self.open_datasets(cosz_f, rsdsdiff_f)[1].data_vars:
            rsdsdiff = self.open_datasets(cosz_f, rsdsdiff_f)[1]
        else:
            print("'rsdsdiff' variable not detected in either dataset")
            sys.exit()
        if 'cosz2' in self.open_datasets(cosz_f, rsdsdiff_f)[0].data_vars:
            cosz = self.open_datasets(cosz_f, rsdsdiff_f)[0]
        elif 'cosz2' in self.open_datasets(cosz_f, rsdsdiff_f)[1].data_vars:
            cosz = self.open_datasets(cosz_f, rsdsdiff_f)[1]
        else:
            print("'cosz2' variable not detected in either dataset")
            sys.exit()
        
        # Save metadata in orig file
        meta = self.get_meta(cosz_f, rsdsdiff_f)
        
        # If lat is -90,90 in cosz lat file, change first and last to -89, 90 respectively (to match CMORized file)
        cosz = self.match_coords(cosz)
        
        # Convert -180-180 lon to 0-360
        cosz = self.lon360(cosz)
        rsdsdiff = self.lon360(rsdsdiff)
        
        # Set cosz 'time' coord as m-d-hour for grouping/matching
        cosz['time'] = self.org_time_cosz(cosz)
        cosz = cosz.rename({'time':'hourofyear'})
        
        # Group rsdsdiff by hourofyear
        rsdsdiff = self.gb_hoy(rsdsdiff)
    
        # Get rsdsdiff*cosz and save dataset
        outds = xr.Dataset()
        product=rsdsdiff*cosz.cosz2
        outds = outds.assign({'rsdsdiff': product.drop('hourofyear')})
        outds = outds.assign({'time_bnds': rsdsdiff_orig.time_bnds})
        outds = outds.assign({'lat_bnds': rsdsdiff_orig.lat_bnds})
        outds = outds.assign({'lon_bnds': rsdsdiff_orig.lon_bnds})
            
        # Replenish original metadata
        outds.lon.attrs = meta.lon.attrs
        outds.lon.encoding = meta.lon.encoding
        outds.rsdsdiff.attrs = meta.rsdsdiff.attrs
        outds.rsdsdiff.encoding = meta.rsdsdiff.encoding
        outds.attrs = meta.attrs
        #outds.attrs['tracking_id']=str(meta.attrs['tracking_id'].split('/')[0] + '/' + str(uuid.uuid1()))
        outds.attrs['contact']= "cmip-giss-l@lists.nasa.gov"
        outds.attrs['license'] = "CMIP6 model data produced by NASA Goddard Institute for Space Studies is released with a Creative Commons Zero Universal (CC0 1.0, https://creativecommons.org/publicdomain/zero/1.0/) and has no restrictions on its use. Consult https://pcmdi.llnl.gov/CMIP6/TermsOfUse for more general terms of use governing CMIP6 output, including citation requirements and proper acknowledgment. Further information about this data can be found via the further_info_url (recorded as a global attribute in this file) and at https:///pcmdi.llnl.gov/. The data producers and data providers make no warranty, either express or implied, including, but not limited to, warranties of merchantability and fitness for a particular purpose. All liabilities arising from the supply of the information (including any liability arising in negligence) are excluded to the fullest extent permitted by law."
        outds.encoding = meta.encoding
        # Supress _Fill_Value from being added to lat and lon
        meta.lat.encoding['_FillValue'] = None
        meta.lon.encoding['_FillValue'] = None
        meta.time.encoding['_FillValue'] = None
        
        # Get encoding
        timecd = meta.time.encoding
        lonecd = meta.lon.encoding
        latecd = meta.lat.encoding
        lonbndsecd = rsdsdiff_orig.lon_bnds.encoding
        lonbndsecd['_FillValue'] = None
        latbndsecd = rsdsdiff_orig.lat_bnds.encoding
        latbndsecd['_FillValue'] = None
        timebndsecd = rsdsdiff_orig.time_bnds.encoding
        timebndsecd['_FillValue'] = None
        rsdsdiffecd = meta.rsdsdiff.encoding
        ecd_master = {'time': timecd,
                      'lon': lonecd,
                      'lat': latecd,
                      'time_bnds': timebndsecd,
                      'lat_bnds': latbndsecd,
                      'lon_bnds': lonbndsecd,
                      'rsdsdiff': rsdsdiffecd}
        
        # Make directory structure
        self.make_dir(rsdsdiff_f, cmordir)
    
        # Save file as a new netcdf
        ncfilename = rsdsdiff_f.split('/')[-1]
        
        outpath = rsdsdiff_f.split(f'{cmordir}/')[1]
    
        outds.to_netcdf(str('rsdsdiff_files/' + outpath), encoding = ecd_master)
        
        return outds


    def check_files(self, rsdsdiff_glob, cosz_f, cmordir, startyear, endyear):
        '''
        1) Check for 3hr/rsdsdiff files for the given run
        
        2) Ensure that rsdsdiff file is within the time range specified by 
        command line arguments
        This will avoid correcting the same file twice without knowing
        if CMORization is split in chunks (e.g. if one had already CMORized 
        1850-1950, then CMORizes 1951-2014, the program will ONLY correct
        rsdsdiff files from 1951-2014)
        This avoids multiplying rsdsdiff files by cosz more than once
        '''
        
        # If 3hr/rsdsdiff files found...
        if len(rsdsdiff_glob) > 0:
            print("Running rsdsdiff correction script...")
            print(f'{len(rsdsdiff_glob)} files detected. Checking for date range compliance and correcting with cosz input...')
            # Iterate through globed files and run() if files pass checks
            for file in rsdsdiff_glob:
                rsdsdiff_file = file
                # Make sure rsdsdiff file is within time range being CMORized
                temp_rsds_ds = xr.open_dataset(rsdsdiff_file)
                # If file within time range specified by CLA, process. Otherwise skip.
                if (temp_rsds_ds.time.values[0].year >= startyear) & (temp_rsds_ds.time.values[-1].year <= endyear):
                    try:
                        # Call the run method on the instance
                        self.run(rsdsdiff_file, cosz_f, cmordir)
                    except Exception as e:
                        print(f'!!!!!!! rsdsdiff step failed for {rsdsdiff_file}. Skipping... !!!!!!!')
                        print("Error:", e)
                else:
                    print(f'!!!!!!! {rsdsdiff_file.split("/")[-1]} not within time range specified in command line arguments ({startyear}, {endyear}). Skipping... !!!!!!!')
        else:
            print("!!!!!!! No 3hr/rsdsdiff files detected. Skipping this step... !!!!!!!")
    
