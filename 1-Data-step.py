import pandas as pd
import numpy as np
import glob
import xarray as xr
import os
import multiprocessing
from dask.delayed import delayed
from dask import compute
from dask.distributed import Client


def get_sst(file_loc, region, region_coords):
    ### Creat new df and open file
    ds = xr.open_dataset(file_loc)
    df = ds.to_dataframe().reset_index()    
    
    ### Convert 0-360 to -180 - 180
    df.loc[:, 'lon'] = np.where(df['lon'] > 180, -360 + df['lon'], df['lon'])

    ### Filter region
    df = df[(df['lat'] >= region_coords[1]) & (df['lat'] <= region_coords[3])]
    df = df[(df['lon'] >= region_coords[0]) & (df['lon'] <= region_coords[2])]
    
    ### New var columns
    df = df.assign(date = df['time'], region = region)
    df = df[['date', 'region', 'lon', 'lat', 'sst']]

    return df



def proc_sst():
    ### SST
    files = sorted(glob.glob('/data2/SST/NOAA_ESRL/DAILY/*.nc'))

    # 2010-2019
    files = files[10:20]

    ### North region
    north_df = [get_sst(x, "North", North) for x in files]
    north_res = pd.concat([d for d in north_df])
    print(f"North Region SST Processed")

    ### West region
    west_df = [get_sst(x, "West", West) for x in files]
    west_res = pd.concat([d for d in west_df])
    print(f"West Region SST Processed")

    ### South region
    south_df = [get_sst(x, "South", South) for x in files]
    south_res = pd.concat([d for d in south_df])
    print(f"South Region SST Processed")

    ### East region
    east_df = [get_sst(x, "East", East) for x in files]
    east_res = pd.concat([d for d in east_df])
    print(f"East Region SST Processed")

    retdat = pd.concat([north_res, west_res, south_res, east_res])
    retdat = retdat.reset_index(drop=True)
    
    return retdat


def get_chl(file_, north_region, south_region, east_region, west_region):
    ### Get year-day
    year_day = os.path.splitext(os.path.basename(file_))[0].split('.')[0][-7:8]
    year = year_day[-7:4]
    day = year_day[5:8]
    month =  pd.to_datetime(year_day, format="%Y%j").month

    ### Get nc file
    ds = xr.open_dataset(file_, drop_variables=['palette'])
    df = ds.to_dataframe().reset_index()

    ### Subset Northern Region
    ndf = df[(df['lat'] >= north_region[1]) & (df['lat'] <= north_region[3])]
    ndf = ndf[(ndf['lon'] >= north_region[0]) & (ndf['lon'] <= north_region[2])]
    ndf = ndf.assign(region = "North", year = year, month=month, day=day)    

    ### Subset Southern Region
    sdf = df[(df['lat'] >= south_region[1]) & (df['lat'] <= south_region[3])]
    sdf = sdf[(sdf['lon'] >= south_region[0]) & (sdf['lon'] <= south_region[2])]
    sdf = sdf.assign(region = "South", year = year, month=month, day=day)   

    ### Subset Eastern Region
    edf = df[(df['lat'] >= east_region[1]) & (df['lat'] <= east_region[3])]
    edf = edf[(edf['lon'] >= east_region[0]) & (edf['lon'] <= east_region[2])]
    edf = edf.assign(region = "East", year = year, month=month, day=day)   

    ### Subset Western Region
    wdf = df[(df['lat'] >= west_region[1]) & (df['lat'] <= west_region[3])]
    wdf = wdf[(wdf['lon'] >= west_region[0]) & (wdf['lon'] <= west_region[2])]
    wdf = wdf.assign(region = "West", year = year, month=month, day=day)   
    
    ### Bind Regions
    outdat = pd.concat([ndf, sdf, edf, wdf]).reset_index(drop=True)
    print(f"{year}" + f"-{month}".zfill(3) + f"-{day}".zfill(3))
    return outdat


def get_wind(dat):
    buoy_id = dat[0]
    year = dat[1]
    region = dat[2]
    for i in range(10):
        try:
            url = f"https://www.ndbc.noaa.gov/data/historical/stdmet/{buoy_id}h{year}.txt.gz"
            df = pd.read_csv(url, compression='gzip', sep=" ", header=0, skipinitialspace=True)
            df = df.iloc[1:]
            date = df['#YY'].astype(str) + "-" + df['MM'].astype(str) + "-" + df['DD'].astype(str)
            df = df.assign(date = date, buoy_id = buoy_id, region = region)
            outdat = df[['date', 'buoy_id', 'region', 'WDIR', 'WSPD']]
            return outdat
        except:
            print(f"Failed: {buoy_id} - {year} - {region} ... retrying {i}")
            continue




if __name__ == "__main__":
    ### Dask setup    
        # NCORES = multiprocessing.cpu_count() - 1

    NCORES = 10
    client = Client(n_workers=NCORES, threads_per_worker=1)


    ### Puerto Rico Regions
    North = [-67.6538, 18.491170, -65.4236, 19.491170]
    West = [-68.1034, 17.7236, -67.1034, 18.74117]
    South = [-67.6538, 17.0288, -65.4236, 18.0288]
    East = [-65.929,  17.7236, -64.929, 18.74117]

    # ### Get SST
    # sst_dat = proc_sst()    
    # sst_dat.to_csv('./data/PR_SST_daily_regional_2010-2019.csv', index=False)

    ### Get CHL
    files = sorted(glob.glob('/data2/CHL/NC/DAILY/*.nc'))
    # files = files[0:5]    
    results = compute([delayed(get_chl)(file_, North, South, East, West) for file_ in files])
    chl_dat = pd.concat([d for d in results[0][:]])
    chl_dat = chl_dat.reset_index(drop=True)
    chl_dat.to_csv('data/PR_CHL_daily_regional_2010_2019.csv', index=False)



    ### Get Wind data
    # Build data frame to loop through
    nbuoys = ["sjnp4", "arop4", "41053"]
    sbuoys = ["mgip4", "42085"]
    wbuoys = ["ptrp4", "41115", "mgzp4"]
    ebuoys = ["41056", "41052", "clbp4", "espp4"]
    nlst_ = [(x, y, "North") for x in nbuoys for y in range(2010, 2019)]
    slst_ = [(x, y, "South") for x in sbuoys for y in range(2010, 2019)]
    wlst_ = [(x, y, "West") for x in wbuoys for y in range(2010, 2019)]
    elst_ = [(x, y, "East") for x in ebuoys for y in range(2010, 2019)]

    ### Build list to compress
    lst_ = nlst_ + slst_ + wlst_ + elst_
    results = compute([delayed(get_wind)(g) for g in lst_])
    wind_dat = pd.concat([d for d in results[0][:]]).reset_index(drop=True)
    wind_dat.to_csv('data/PR_Wind_daily_regional_2010-2019', index=False)


    ### Hurricane data
    # August 2014; August 2015; Sept 2017; July 2018; August 2019; Sept 2019
    hurr_events = ['2014-08', '2015-08', '2017-09', '2018-07', '2019-08', '2019-09']
    dates = [i.strftime("%Y-%m") for i in pd.date_range(start="2010-01-01", end="2019-12-01", freq='MS')]
    hurr_dat = pd.DataFrame({'date': dates, "hurricane": 0})
    hurr_dat = hurr_dat.assign(hurricane = np.where(hurr_dat['date'].isin(hurr_events), 1, 0))
    hurr_dat.to_csv('data/PR_Hurricane_daily_regional_2010-2019', index=False)
    





    ### -------------------------------------------------------------------------------------------
    ### Annual estimates

    ### Merge data into panel set
    sst = pd.read_csv('./data/PR_SST_daily_regional_2010-2019.csv', index_col=False)
    chl = pd.read_csv('data/PR_CHL_daily_regional_2010_2019.csv', index_col=False)
    wind = pd.read_csv('data/PR_Wind_daily_regional_2010-2019', index_col=False)
    hurr = pd.read_csv('data/PR_Hurricane_daily_regional_2010-2019', index_col=False)
    noi = pd.read_csv('data/cciea_OC_NOI.csv', index_col=False, skiprows=1, usecols=[0, 1])
    

    # SST
    sst = sst.assign(month = pd.to_datetime(sst['date']).dt.month, year = pd.to_datetime(sst['date']).dt.year)
    sst = sst.groupby(['year', 'region']).agg({'sst': 'mean'}).reset_index()
    
    # CHL
    chl = chl.groupby(['year', 'region']).agg({'chlor_a': 'mean'}).reset_index()

    # Wind
    wind = wind.assign(month = pd.to_datetime(wind['date']).dt.month, year = pd.to_datetime(wind['date']).dt.year)
    wind = wind.groupby(['year', 'region']).agg({'WSPD': 'mean'}).reset_index()

    # Hurricane
    hurr = hurr.assign(month = pd.to_datetime(hurr['date']).dt.month, year = pd.to_datetime(hurr['date']).dt.year)
    hurr['hurricane'] = np.where(hurr['hurricane'] >= 1, 1, 0)
    
    # NOI
    noi.columns = ['date', 'noi']
    noi = noi.assign(month = pd.to_datetime(noi['date']).dt.month, year = pd.to_datetime(noi['date']).dt.year)
    noi = noi[noi['year'] >= 2010].groupby('year').agg({'noi': 'sum'}).reset_index()







    # Regression data
    regdat = pd.read_csv('data/effort_region_conflict_reg_data.csv')
    regdat = regdat.groupby(['year', 'region']).agg({'effort': 'sum', 'coop_sum': 'sum', 'conf_sum': 'sum'}).reset_index()
    
    
    regdat['cc_ratio'] = regdat['conf_sum'] / regdat['coop_sum']

    regdat = regdat.assign(cc_ratio = np.where(regdat['coop_sum'] == 0, 0, regdat['cc_ratio']))
    
    regdat['region'] = np.where(regdat['region'] == 'E', 'East', regdat['region'])
    regdat['region'] = np.where(regdat['region'] == 'W', 'West', regdat['region'])
    regdat['region'] = np.where(regdat['region'] == 'N', 'North', regdat['region'])
    regdat['region'] = np.where(regdat['region'] == 'S', 'South', regdat['region'])        
    regdat = regdat[['year', 'region', 'effort', 'cc_ratio']]

    # Bind data
    regdat = regdat.merge(sst, how='left', on=['year', 'region'])
    regdat = regdat.merge(chl, how='left', on=['year', 'region'])
    regdat = regdat.merge(wind, how='left', on=['year', 'region'])
    regdat = regdat.merge(hurr, how='left', on=['year'])
    regdat = regdat.merge(noi, how='left', on=['year'])


    regdat.to_csv("data/PR_regdat.csv", index=False)