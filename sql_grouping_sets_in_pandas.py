import itertools as it
import pandas as pd

from pandas.util.testing import assert_frame_equal

def powerset(iterable): 
    ''' https://docs.python.org/3/library/itertools.html#itertools-recipes
        powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)
    '''
    s = list(iterable)
    return it.chain.from_iterable(it.combinations(s,r) for r in range(len(s)+1))

def grouper(df,grpby=None,aggfunc=None):
    ''' produces aggregate DataFrame from DataFrames for non-redundant groupings
        workingdf is used to avoid modifying original DataFrame
    '''
    uniqcols = (i for i in grpby if len(df[i].unique()) == 1)
    pwrset = (i for i in powerset(grpby))
    s = set()
    for uniqcol in uniqcols:
        for i in pwrset:
            if uniqcol in i:
                # add level of aggregation only when non-redundant
                s.add(i) 
    workingdf = df.copy()
    for idx,i in enumerate(s):
        print('  grouping by: {}'.format(list(i)))
        if i != (): 
            tmp = aggfunc( workingdf.groupby(list(i)) )
        else:
            # hack to get output to be a DataFrameGroupBy object:
            #   insert dummy column on which to group by
            # old, naive code:
            #   tmp = aggfunc( workingdf )
            dummycolname = hash(tuple(workingdf.columns.tolist()))
            workingdf[dummycolname] = ''
            tmp = aggfunc( workingdf.groupby(dummycolname) )
        # drop the index and add it back
        if i == (): tmp.reset_index(drop=True,inplace=True)
        else: tmp.reset_index(inplace=True)
        for j in grpby: 
            if j not in tmp: # if column is not in DataFrame add it
                tmp[j] = '(All)'
        # new list with all columns including aggregate ones; do this only once
        if idx == 0:
            finalcols = grpby[:]
            addlcols = [k for k in tmp if k not in grpby] # aggregate columns 
            finalcols.extend(addlcols)
        # reorder columns 
        tmp = tmp[finalcols] 
        # creation of final DataFrame
        if idx == 0:
            final = tmp; del tmp
        else: 
            final = pd.concat( [final,tmp] ); del tmp
    del workingdf
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
    df = pd.DataFrame({'Area':['a','a','b',],
                       'Year':[2014,2014,2014,],
                       'Month':[1,2,3,],
                       'Total':[4,5,6,],})
    final = grouper(df,grpby=['Area','Year'],aggfunc=agg)
    print(final)
    # test against expected result 
    expected = '''{"Area":{"0":"(All)","1":"a","2":"b"},
                   "Year":{"0":2014,"1":2014,"2":2014},
                   "Total (n)":{"0":15,"1":9,"2":6}}'''
    testfinal = pd.read_json(expected)
    testfinal = testfinal[final.columns.tolist()] # reorder columns 
    try:
        # check_names kwarg True: compare indexes and columns 
        assert_frame_equal(final,testfinal,check_names=True)
    except AssertionError as e:
        print(e)
