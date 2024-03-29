#%%
import pandas as pd
import numpy as np
import glob
# import xarray as xr
import os
import multiprocessing
import requests
from dask import delayed, compute
# from dask.distributed import Client
# from distributed import Client
import urllib.request 


#%%
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


#%%
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


#%%
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



#%%
def get_wind(dat):
    buoy_id = dat[0]
    year = dat[1]
    region = dat[2]
    for i in range(10):
        try:
            url = f"https://www.ndbc.noaa.gov/data/historical/stsdmet/{buoy_id}h{year}.txt.gz"
            df = pd.read_csv(url, compression='gzip', sep=" ", header=0, skipinitialspace=True)
            df = df.iloc[1:]
            date = df['#YY'].astype(str) + "-" + df['MM'].astype(str) + "-" + df['DD'].astype(str)
            df = df.assign(date = date, buoy_id = buoy_id, region = region)
            outdat = df[['date', 'buoy_id', 'region', 'WDIR', 'WSPD']]
            return outdat
        except:
            print(f"Failed: {buoy_id} - {year} - {region} ... retrying {i}")
            continue


#%%
def download_ssh_data():
    url = "https://podaac.jpl.nasa.gov/ws/search/granule?datasetId=PODAAC-SLREF-CDRV2&startTime=2009-12-01&endTime=2019-12-31&itemsPerPage=2000&sortBy=ascending&format=html&pretty=true"
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'lxml')   
    files = []
    for heading in soup.find_all(["h2"]):
        files.extend(heading)
        
    files = sorted(files)
    
    for file_ in files:
        filename = f"https://podaac-opendap.jpl.nasa.gov/opendap/allData/merged_alt/L4/cdr_grid/{file_}"
        urllib.request.urlretrieve(filename, f"/data2/SSH/PODACC/{file_}")
    print(file_)
        
        
#%%
def get_ssh(file_, north_region, south_region, east_region, west_region):
    print(file_)
    ds = xr.open_dataset(file_)
    df = ds.to_dataframe().reset_index()

    df = df[['Time', 'Latitude', 'Longitude', 'SLA', 'SLA_ERR']]
    df = df.rename(columns={'Time': 'date', 'Latitude': 'lat', 'Longitude': 'lon', 'SLA': 'sla', 'SLA_ERR': 'sla_err'})

    ### Rework -360 longitude
    df.loc[:, 'lon'] = np.where(df['lon'] > 180, -360 + df['lon'], df['lon'])
    df = df.dropna()

    year = pd.to_datetime(df.date).dt.year
    month = pd.to_datetime(df.date).dt.month
    day = pd.to_datetime(df.date).dt.day

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
    outdat = outdat.assign(date = outdat['date'].dt.normalize())
    outdat = outdat.drop_duplicates()
    return outdat


#%%
def proc_cc(start_date, end_date, cc_int, n_region):
    # start_date = ccdat.sdate[0]
    # end_date = ccdat.edate[0]
    # cc_int = ccdat.intensity[0]
    # n_region = ccdat.region[2]

    outdat = pd.DataFrame()
    for region_ in n_region.split(','):
        region_ = region_.strip()
        indat = pd.DataFrame({'date': pd.DatetimeIndex(pd.date_range(start_date, end_date, freq='M').strftime("%Y-%m"))})    
        indat = indat.assign(year = indat.date.dt.year,
                             month = indat.date.dt.month,
                             region = region_,
                             intensity = cc_int)
        indat = indat[indat['year'] >= 2010]
        indat = indat.assign(timeSeries = range(1, 1 + len(indat)))
        outdat = pd.concat([outdat, indat])
    return outdat


   
    
#%%
if __name__ == "__main__":
    # ### Dask setup    
    # NCORES = multiprocessing.cpu_count() - 1
    # NCORES = 30
    # client = Client(n_workers=NCORES, threads_per_worker=1)

    # ### Puerto Rico Regions
    # North = [-67.6538, 18.491170, -65.4236, 19.491170]
    # West = [-68.1034, 17.7236, -67.1034, 18.74117]
    # South = [-67.6538, 17.0288, -65.4236, 18.0288]
    # East = [-65.929,  17.7236, -64.929, 18.74117]

    # # ### Get SST
    # sst_dat = proc_sst()    
    # sst_dat.to_csv('./data/PR_SST_daily_regional_2010-2019.csv', index=False)


    # ### Get CHL
    # files = sorted(glob.glob('/data2/CHL/NC/DAILY/*.nc'))
    # results = compute([delayed(get_chl)(file_, North, South, East, West) for file_ in files])
    # chl_dat = pd.concat([d for d in results[0][:]])
    # chl_dat = chl_dat.reset_index(drop=True)
    # chl_dat.to_csv('data/PR_CHL_daily_regional_2010_2019.csv', index=False)



    # ### Get Wind data
    # # Build data frame to loop through
    # nbuoys = ["sjnp4", "arop4", "41053"]
    # sbuoys = ["mgip4", "42085"]
    # wbuoys = ["ptrp4", "41115", "mgzp4"]
    # ebuoys = ["41056", "41052", "clbp4", "espp4"]
    # nlst_ = [(x, y, "North") for x in nbuoys for y in range(2010, 2019)]
    # slst_ = [(x, y, "South") for x in sbuoys for y in range(2010, 2019)]
    # wlst_ = [(x, y, "West") for x in wbuoys for y in range(2010, 2019)]
    # elst_ = [(x, y, "East") for x in ebuoys for y in range(2010, 2019)]



    # ### Build list to compress
    # lst_ = nlst_ + slst_ + wlst_ + elst_
    # results = compute([delayed(get_wind)(g) for g in lst_])
    # wind_dat = pd.concat([d for d in results[0][:]]).reset_index(drop=True)
    # wind_dat.to_csv('data/PR_Wind_daily_regional_2010-2019', index=False)


    # ### Hurricane data
    # # August 2014; August 2015; Sept 2017; July 2018; August 2019; Sept 2019
    # hurr_events = ['2014-08', '2015-08', '2017-09', '2018-07', '2019-08', '2019-09']
    # dates = [i.strftime("%Y-%m") for i in pd.date_range(start="2010-01-01", end="2019-12-01", freq='MS')]
    # hurr_dat = pd.DataFrame({'date': dates, "hurricane": 0})
    # hurr_dat = hurr_dat.assign(hurricane = np.where(hurr_dat['date'].isin(hurr_events), 1, 0))
    # hurr_dat.to_csv('data/PR_Hurricane_daily_regional_2010-2019', index=False)
    


    # ### Get Sea Surface Height
    # # Download PODACC files
    # # download_ssh_data()
    
    # # Get files from download
    # files = glob.glob('/data2/SSH/PODACC/*.nc')
    
    # ### Build list to compress
    # # results = [get_ssh(file_, North, South, East, West) for file_ in files]
    # results = compute([delayed(get_ssh)(file_, North, South, East, West) for file_ in files])
    # ssh_dat = pd.concat([d for d in results[0][:]]).reset_index(drop=True)
    # ssh_dat.to_csv('data/PR_SSH_5day_regional_2010-2019', index=False)



    # Get area of region
    North = [-67.6538, 18.491170, -65.4236, 19.491170]
    West = [-68.1034, 17.7236, -67.1034, 18.74117]
    South = [-67.6538, 17.0288, -65.4236, 18.0288]
    East = [-65.929,  17.7236, -64.929, 18.74117]

    area_df = pd.DataFrame({'region': ["North", "West", "South", "East"],
                            'area': [(North[2] - North[0]) * (North[3] - North[1]),
                                    (West[2] - West[0]) * (West[3] - West[1]),
                                    (South[2] - South[0]) * (South[3] - South[1]),
                                    (East[2] - East[0]) * (East[3] - East[1])]})






    # -------------------------------------------------------------------------
    # Monthly estimates

    # Merge data into panel set
    sst = pd.read_csv('data/PR_SST_daily_regional_2010-2019.csv', index_col=False)
    chl = pd.read_csv('data/PR_CHL_daily_regional_2010_2019.csv', index_col=False)
    wind = pd.read_csv('data/PR_Wind_daily_regional_2010-2019', index_col=False)
    # hurr = pd.read_csv('data/PR_Hurricane_daily_regional_2010-2019', index_col=False)
    hurr = pd.read_csv('data/hurricanes_138km.csv', index_col=False)
    noi = pd.read_csv('data/NOI_Index.csv')
    ssh = pd.read_csv('data/PR_SSH_5day_regional_2010-2019', index_col=False)

    # SST
    sst = sst.assign(month=pd.to_datetime(sst['date']).dt.month, year=pd.to_datetime(sst['date']).dt.year)
    sst = sst.groupby(['year', 'month', 'region']).agg({'sst': ['mean', 'var']}).reset_index()
    sst.columns = ['year', 'month', 'region', 'sst_mean', 'sst_var']

    # CHL
    chl = chl.groupby(['year', 'month', 'region']).agg({'chlor_a': ['mean', 'var']}).reset_index()
    chl.columns = ['year', 'month', 'region', 'chlor_a_mean', 'chlor_a_var']

    # Wind
    wind = wind.assign(month=pd.to_datetime(wind['date']).dt.month, year=pd.to_datetime(wind['date']).dt.year)
    wind = wind.groupby(['year', 'month', 'region']).agg({'WSPD': 'mean'}).reset_index()

    # Hurricane
    # hurr = hurr.assign(month=pd.to_datetime(hurr['date']).dt.month, year=pd.to_datetime(hurr['date']).dt.year)
    # hurr['hurricane'] = np.where(hurr['hurricane'] >= 1, 1, 0)
    
    # SSH
    ssh = ssh[ssh['year'] >= 2010].groupby(['year', 'month', 'region']).agg({'sla': 'mean', 'sla_err': 'mean'}).reset_index()

    # ------------------------------------------------
    # Monthly Puerto Rico Data    
    efdat = pd.read_csv('data/PR_nonconf_landings_2010_19_2021-01-07.CSV')
    cdat = pd.read_csv('./data/Municipalities_by_region_Puerto_Rico_wideFormat.csv')
    ccdat = pd.read_csv('data/FCCE_Master_Intensity_Expanded_zeros_excluded.csv')
    # ccdat = pd.read_csv('data/FCCE_Master_Intensity_Expanded.csv')
    pricedat = pd.read_csv('data/PR_Fish_Species_Prices_2010_2018.csv')

    # [1] Clean Intensity data (dependent variables)
    # Index(['YEAR_LANDED', 'MONTH_LANDED', 'LANDING_LOCATION_COUNTY',
    #    'SPECIES_ITIS', 'ITIS_COMMON_NAME', 'ITIS_SCIENTIFIC_NAME',
    #    'POUNDS_LANDED', 'ADJUSTED_POUNDS', 'trips', 'fishers'],
    #   dtype='object')
    
    ccdat = ccdat.assign(year = pd.DatetimeIndex(ccdat['EndDate']).year)
    # ccdat = ccdat[ccdat['year'] >= 2010]
    ccdat = ccdat[['StartDate', 'EndDate', 'DNER_Districts', 'Intensity_Score', 'CoopCon']]
    ccdat.columns = ['sdate', 'edate', 'region', 'intensity', 'coop_con']
    
    # Aggregate conflict/coop and get ratio
    ccdat2 = ccdat.apply(lambda x: proc_cc(x['sdate'], x['edate'], x['intensity'], x['region']), axis=1)
    ccdat2 = pd.concat([x for x in ccdat2])

    # Keep full df
    main_ccdat2 = ccdat2
    main_ccdat2 = main_ccdat2.assign(region = main_ccdat2['region'].str.title())

    # Aggreagete df
    ccdat2 = ccdat2.assign(group_1 = np.where(ccdat2['intensity'] <= -1, 0, 1))

    # Conflict count
    conflict_count = ccdat2[ccdat2['group_1'] == 0].groupby(['year', 'month', 'region'])['group_1'].count().reset_index()
    conflict_sum = ccdat2[ccdat2['group_1'] == 0].groupby(['year', 'month', 'region'])['intensity'].sum().reset_index()
    conflict_mean = ccdat2[ccdat2['group_1'] == 0].groupby(['year', 'month', 'region'])['intensity'].mean().abs().reset_index()
    
    conflict_count = conflict_count.rename(columns={conflict_count.columns[-1]: 'conflict_count'})
    conflict_sum = conflict_sum.rename(columns={conflict_sum.columns[-1]: 'conflict_sum'})
    conflict_mean = conflict_mean.rename(columns={conflict_mean.columns[-1]: 'conflict_mean'})

    # Coop count
    coop_count = ccdat2[ccdat2['group_1'] == 1].groupby(['year', 'month', 'region'])['group_1'].count().reset_index()
    coop_sum = ccdat2[ccdat2['group_1'] == 1].groupby(['year', 'month', 'region'])['intensity'].sum().reset_index()
    coop_mean = ccdat2[ccdat2['group_1'] == 1].groupby(['year', 'month', 'region'])['intensity'].mean().abs().reset_index()
    
    coop_count = coop_count.rename(columns={coop_count.columns[-1]: 'coop_count'})
    coop_sum = coop_sum.rename(columns={coop_sum.columns[-1]: 'coop_sum'})
    coop_mean = coop_mean.rename(columns={coop_mean.columns[-1]: 'coop_mean'})
    
    # Merge
    ccdat3 = conflict_count.merge(conflict_sum, on=['year', 'month','region'], how='left')
    ccdat3 = ccdat3.merge(conflict_mean, on=['year', 'month','region'], how='left')
    ccdat3 = ccdat3.merge(coop_count, on=['year', 'month','region'], how='left')
    ccdat3 = ccdat3.merge(coop_sum, on=['year', 'month','region'], how='left')
    ccdat3 = ccdat3.merge(coop_mean, on=['year', 'month','region'], how='left')

    # Get ratios    
    ccdat3 = ccdat3.assign(cc_ratio_count = ccdat3['conflict_count'] / ccdat3['coop_count'])
    ccdat3 = ccdat3.assign(cc_ratio_sum = ccdat3['conflict_sum'] / ccdat3['coop_sum'])
    ccdat3 = ccdat3.assign(cc_ratio_mean = ccdat3['conflict_mean'] / ccdat3['coop_mean'])
    
    # Filter 2010
    # ccdat3 = ccdat3[(ccdat3['year'] >= 2010) & (ccdat3['year'] <= 2017)]
    ccdat3 = ccdat3.assign(region = ccdat3['region'].str.title())
    
    # [2] Clean Fishing effort data
    # Index(['YEAR_LANDED', 'MONTH_LANDED', 'LANDING_LOCATION_COUNTY',
    #    'SPECIES_ITIS', 'ITIS_COMMON_NAME', 'ITIS_SCIENTIFIC_NAME',
    #    'POUNDS_LANDED', 'ADJUSTED_POUNDS', 'trips', 'fishers'],
    #   dtype='object')
    
    species = efdat.groupby(['ITIS_COMMON_NAME']).agg({'ADJUSTED_POUNDS': 'sum'}).sort_values('ADJUSTED_POUNDS', ascending=False).head(300).reset_index()
    # species = species[~species['ITIS_COMMON_NAME'].isin(['LOBSTERS,SPINY', 'CONCH,QUEEN', 'OCTOPUS,UNSPECIFIED'])]
    species = species['ITIS_COMMON_NAME'].ravel()
    print(species)

    # Get top 10 species
    top10_species = efdat.groupby('ITIS_COMMON_NAME').sum().reset_index().sort_values('POUNDS_LANDED', ascending=False).head(10)['ITIS_COMMON_NAME']
    efdat_top10 = efdat[efdat['ITIS_COMMON_NAME'].isin(top10_species)]
    efdat_top10 = efdat_top10.groupby(['YEAR_LANDED', 'ITIS_COMMON_NAME']).sum().reset_index()
    efdat_top10.to_csv('data/top10_catch_species.csv', index=False)

    # Calc perc catch from subset
    fil_spec = efdat[efdat['ITIS_COMMON_NAME'].isin(species)].groupby('YEAR_LANDED')['ADJUSTED_POUNDS'].sum().reset_index()
    full_spec = efdat.groupby('YEAR_LANDED')['ADJUSTED_POUNDS'].sum().reset_index()
    calc_perc = fil_spec.merge(full_spec, on='YEAR_LANDED')                      
    calc_perc = calc_perc.assign(perc = calc_perc['ADJUSTED_POUNDS_x'] / calc_perc['ADJUSTED_POUNDS_y'])
    print(calc_perc)
    print(f"Average catch in sample: {np.round(calc_perc.perc.mean(), 3)*100}%")
    
    #        YEAR_LANDED  ADJUSTED_POUNDS_x  ADJUSTED_POUNDS_y      perc
    # 0         2010       1.334739e+06       1.857560e+06  0.718544
    # 1         2011       1.033816e+06       1.549942e+06  0.667003
    # 2         2012       1.349670e+06       2.092686e+06  0.644946
    # 3         2013       8.970195e+05       1.477108e+06  0.607281
    # 4         2014       1.185775e+06       1.847903e+06  0.641687
    # 5         2015       1.208846e+06       1.918117e+06  0.630226
    # 6         2016       1.098157e+06       1.884955e+06  0.582590
    # 7         2017       7.714999e+05       1.308595e+06  0.589563
    # 8         2018       9.707479e+05       1.825380e+06  0.531806
    # 9         2019       1.261108e+06       1.928243e+06  0.654019
    
    # array(['LOBSTERS,SPINY', 'CONCH,QUEEN', 'SNAPPER,SILK', 'SNAPPER,QUEEN',
    #    'SNAPPER,YELLOWTAIL', 'SNAPPER,LANE', 'DOLPHINFISH',
    #    'TRIGGERFISH,QUEEN', 'HOGFISH', 'BOXFISH,UNSPECIFIED',
    #    'GROUPER,RED HIND', 'TUNA,BLACKFIN', 'SNAPPER,MUTTON', 'BALLYHOO',
    #    'PARROTFISHES,UNSPECIFIED', 'SNAPPER,UNSPECIFIED',
    #    'OCTOPUS,UNSPECIFIED', 'MACKEREL,KING', 'JACK,BAR',
    #    'TUNA,SKIPJACK', 'MACKEREL,CERO', 'WAHOO', 'HERRING,SARDINELLA',
    #    'TUNNY,LITTLE', 'PORGY,UNSPECIFIED', 'GRUNT,UNSPECIFIED',
    #    'SNAPPER,CARDINAL', 'MULLET,WHITE', 'SNAPPER,VERMILION',
    #    'GRUNT,WHITE'], dtype=object)
    
    pricedat = pd.read_csv('data/PR_Fish_Species_Prices_2010_2018.csv')

    # Clean pricing data
    pricedat = pricedat.set_index('SPECIES').unstack().reset_index()
    pricedat.columns = ['year_region', 'species', 'price']
    pricedat = pricedat.assign(year_region = pricedat['year_region'].str.replace("-", ""))
    pricedat = pricedat.assign(region = pricedat['year_region'].str[-1:])
    pricedat = pricedat.assign(year = pricedat['year_region'].str[0:-1])
    pricedat = pricedat.drop(columns=['year_region']).reset_index(drop=True)
    pricedat = pricedat.assign(species = pricedat['species'].str.strip())
    pricedat = pricedat.assign(price = pricedat['price'].replace("  ", '0'))
    pricedat = pricedat.dropna()

    # Convert regions to full names
    pricedat = pricedat.assign(region = np.where(pricedat['region'] == "N", "North", pricedat['region']))
    pricedat = pricedat.assign(region = np.where(pricedat['region'] == "S", "South", pricedat['region']))
    pricedat = pricedat.assign(region = np.where(pricedat['region'] == "E", "East", pricedat['region']))
    pricedat = pricedat.assign(region = np.where(pricedat['region'] == "W", "West", pricedat['region']))
    # pricedat['species'] = pricedat['species'].astype(str)
    pricedat['year'] = pricedat['year'].astype(int)
    pricedat['price'] = pricedat['price'].astype(float)
    pricedat['species'].unique()

    # Average each species price
    pricedat = pricedat.groupby(['species', 'region']).agg({'price': 'mean'}).reset_index()
    
    # Filter top 30 species
    efdat2 = efdat[efdat['ITIS_COMMON_NAME'].isin(species)]
    
    efdat2 = efdat2[['YEAR_LANDED', 'MONTH_LANDED', 'LANDING_LOCATION_COUNTY', 'ITIS_COMMON_NAME', 'ADJUSTED_POUNDS', 'trips', 'fishers']]
    efdat2.columns = ['year', 'month', 'county', 'species', 'pounds', 'trips', 'fishers']
    efdat2 = efdat2.merge(cdat, on=['county'], how='left')

    efdat2 = efdat2.assign(species = efdat2['species'].str.strip())

    # Merge in prices
    efdat2 = efdat2.merge(pricedat, on=['species', 'region'], how='left')

    # Drop county and species for aggregation
    efdat2 = efdat2.drop(columns=['county', 'species'])
    
    # aggregate species
    efdat3 = efdat2.groupby(['year', 'month', 'region']).agg({'pounds': 'sum', 'trips': 'sum', 'fishers': 'sum', 'price': 'mean'})
    efdat3 = efdat3.reset_index()

    efdat3.to_csv('data/PR_EFFORT_PRICES.csv', index=False)

    # [3] Merge all data
    regdat = efdat3.merge(ccdat3, on=['year', 'month', 'region'], how='left')
    regdat = regdat.merge(sst, on=['year', 'month', 'region'], how='left')
    regdat = regdat.merge(chl, on=['year', 'month', 'region'], how='left')
    regdat = regdat.merge(wind, on=['year', 'month', 'region'], how='left')
    regdat = regdat.merge(ssh, on=['year', 'month', 'region'], how='left')
    regdat = regdat.merge(hurr, on=['year', 'month', 'region'], how='left')
    regdat = regdat.merge(noi, on=['year', 'month'], how='left')
    regdat = regdat.merge(area_df, on=['region'], how='left')
    regdat['hurricane'] = regdat.hurricane.fillna(0)
    regdat = regdat.dropna()
    
    print("Saving: 'data/FULL_PR_regdat_monthly.csv'")
    regdat.to_csv('data/FULL_PR_regdat_monthly.csv', index=False)

    # Keep df with all ob
    mregdat = efdat3.merge(main_ccdat2, on=['year', 'month', 'region'], how='left')
    mregdat = mregdat.merge(sst, on=['year', 'month', 'region'], how='left')
    mregdat = mregdat.merge(chl, on=['year', 'month', 'region'], how='left')
    mregdat = mregdat.merge(wind, on=['year', 'month', 'region'], how='left')
    mregdat = mregdat.merge(ssh, on=['year', 'month', 'region'], how='left')
    mregdat = mregdat.merge(hurr, on=['year', 'month', 'region'], how='left')
    mregdat = mregdat.merge(noi, on=['year', 'month'], how='left')
    mregdat = mregdat.merge(area_df, on=['region'], how='left')
    mregdat['hurricane'] = mregdat.hurricane.fillna(0)
    mregdat = mregdat.dropna()
    
    print("Saving: 'data/UNAGG_PR_regdat_monthly.csv'")
    mregdat.to_csv('data/UNAGG_PR_regdat_monthly.csv', index=False)


    