import itertools as it
import pandas as pd

from pandas.testing import assert_frame_equal

def powerset(iterable): 
    ''' https://docs.python.org/3/library/itertools.html#itertools-recipes
        powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)
    '''
    s = list(iterable)
    return it.chain.from_iterable(it.combinations(s,r) for r in range(len(s)+1))

def grouper(df,grpby=None,aggfunc=None):
    ''' produces aggregate DataFrame from DataFrames for non-redundant groupings
        workingdf is used to avoid modifying original DataFrame

        Optimization: groupings that differ only by unique-value columns produce
        identical aggregations, so we compute each effective grouping only once.
    '''
    uniqcols = set(col for col in grpby if len(df[col].unique()) == 1)
    uniq_values = {col: df[col].unique()[0] for col in uniqcols}
    if uniqcols:
        # Filter to groupings containing at least one unique column
        s = [cols for cols in powerset(grpby) if not uniqcols.isdisjoint(cols)]
    else:
        # No unique columns - use all groupings from powerset
        s = list(powerset(grpby))

    # Group the groupings by their effective grouping (with unique columns removed)
    effective_to_groupings = {}
    for cols in s:
        effective = tuple(c for c in cols if c not in uniqcols)
        if effective not in effective_to_groupings:
            effective_to_groupings[effective] = []
        effective_to_groupings[effective].append(cols)

    print('  {} groupings reduced to {} aggregations'.format(len(s), len(effective_to_groupings)))

    workingdf = df.copy()

    # Perform aggregation once per unique effective grouping
    agg_results = {}
    for effective in effective_to_groupings:
        print('  aggregating by: {}'.format(list(effective)))
        if effective:
            agg_results[effective] = aggfunc(workingdf.groupby(list(effective)))
        else:
            # Grand total: use dummy column to get DataFrameGroupBy object
            dummycolname = hash(tuple(workingdf.columns.tolist()))
            workingdf[dummycolname] = ''
            agg_results[effective] = aggfunc(workingdf.groupby(dummycolname))

    # Build output rows for each original grouping using cached aggregation results
    frames = []
    finalcols = None
    for effective, groupings in effective_to_groupings.items():
        base_result = agg_results[effective]
        if effective == ():
            base_result = base_result.reset_index(drop=True)
        else:
            base_result = base_result.reset_index()

        for cols in groupings:
            tmp = base_result.copy()
            # Add unique columns that were in this grouping
            for col in cols:
                if col not in tmp.columns:
                    tmp[col] = uniq_values[col]
            # Add '(All <Column>s)' for columns not in the grouping
            for col in grpby:
                if col not in tmp.columns:
                    tmp[col] = '(All {}s)'.format(col)

            # Determine final column order once
            if finalcols is None:
                finalcols = grpby[:]
                addlcols = [k for k in tmp.columns if k not in grpby]
                finalcols.extend(addlcols)

            tmp = tmp[finalcols]
            frames.append(tmp)

    del workingdf
    final = pd.concat(frames, ignore_index=True)
    final = final.sort_values(by=finalcols)
    final.reset_index(drop=True,inplace=True)
    return final

def agg(grpbyobj):
    ''' the purpose of this function is to: 
          specify aggregate operation(s) you wish to perform, 
          name the resulting column(s) in the final DataFrame.
    '''
    tmp = pd.DataFrame()
    tmp['Total (n)'] = grpbyobj['Total'].sum()
    return tmp

if __name__ == '__main__':
    df = pd.DataFrame({'Area':['a','a','b','b'],
                       'Year':[2014,2014,2014,2015],
                       'Month':[1,2,3,4],
                       'Total':[5,6,7,8],})
    final = grouper(df,grpby=['Area','Year'],aggfunc=agg)
    print(final)
    # test against expected result
    expected = '''{
        "Area":{"0":"(All Areas)","1":"(All Areas)","2":"(All Areas)","3":"a","4":"a","5":"b","6":"b","7":"b"},
        "Year":{"0":2014,"1":2015,"2":"(All Years)","3":2014,"4":"(All Years)","5":2014,"6":2015,"7":"(All Years)"},
        "Total (n)":{"0":18,"1":8,"2":26,"3":11,"4":11,"5":7,"6":8,"7":15}
    }'''
    testfinal = pd.read_json(expected)
    testfinal = testfinal[final.columns.tolist()] # reorder columns 
    try:
        # check_names kwarg True: compare indexes and columns 
        assert_frame_equal(final,testfinal,check_names=True)
    except AssertionError as e:
        print(e)
