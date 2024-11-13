# ---------------------------------------------------------------------------------
# Takes in two feature classes with the same schema and checks every row for differences
# or new rows. The output is a list of rows from both inputs in the form of CSV.
# The output will include the different or new rows from both inputs with a column
# that specifies the feature class name and the object ID.
#
# email merielle@cityofhurricane.com
# linkedin https://www.linkedin.com/in/merielle-redwine-a4a937ab/
# ---------------------------------------------------------------------------------
import arcpy
import os
import pandas as pd
# Input feature classes
input1 = arcpy.GetParameter(0)
input2 = arcpy.GetParameter(1)
fields_to_compare = arcpy.GetParameter(2)
# Output feature class
outputPath = arcpy.GetParameter(3)
# list fields in the feature class as objects
fields1 = arcpy.ListFields(input1)
fields2 = arcpy.ListFields(input2)
layer1name = input1.name
# extract names from field objects and create sorted field lists with OID first
listFields1 = []
for field in fields1:
    if field.name in fields_to_compare:
        listFields1.append(field.name)
    elif field.name == 'objectid' or field.name == 'OBJECTID':
        oid1 = field.name
listFields1.sort()
arcpy.AddMessage(f"list fields1: {input1} {listFields1}")
listFields2 = []
for field in fields2:
    if field.name in fields_to_compare:
        listFields2.append(field.name)
    elif field.name == 'objectid' or field.name == 'OBJECTID':
        oid2 = field.name
listFields2.sort()
arcpy.AddMessage(f"list fields2: {input2} {listFields2}")
fieldsIntersect = list(set(listFields1) & set(listFields2))
fieldsIntersect.sort()
arcpy.AddMessage(f"intersect {fieldsIntersect}")
# Output table
outputDf1 = []
outputDf2 = []
# Use a search cursor to iterate over each feature and append it to the list
with arcpy.da.SearchCursor(input1, [oid1]+fieldsIntersect) as cursor:
    for row in cursor:
        outputDf1.append(row)
        # Convert to a DataFrame
df1 = pd.DataFrame(outputDf1, columns=[oid1]+fieldsIntersect)
df1 = df1.sort_values(by='development')
arcpy.AddMessage(f"df1: {df1}")
# Use a search cursor to iterate over each feature and append it to the list
with arcpy.da.SearchCursor(input2, [oid2]+fieldsIntersect) as cursor:
    for row in cursor:
        outputDf2.append(row)
df2 = pd.DataFrame(outputDf2, columns=[oid2]+fieldsIntersect)
df2 = df2.sort_values(by='development')
arcpy.AddMessage(f"df2: {df2}")
df3 = pd.merge(df1, df2, how='outer', on=fieldsIntersect, indicator=True)
# Filter rows where there is no match in the other DataFrame
dfnew = df3[df3['_merge'] != 'both']
# Replace 'text' with 'newtext' in the 'fieldnew' column
dfnew['_merge'] = dfnew['_merge'].replace('left_only', str(input1))
dfnew['_merge'] = dfnew['_merge'].replace('right_only', str(input2))
# Create a new 'oid' column by choosing oid1 or oid2 depending on which DataFrame had the extra row
dfnew['oid'] = df3.apply(lambda row: row[oid1] if pd.notna(row[oid1]) else row[oid2], axis=1)
# Drop the columns no longer needed
dfnew = dfnew[['oid']+fieldsIntersect+['_merge']]
# Set 'oid' as the new index
dfnew = dfnew.set_index('oid')
arcpy.AddMessage(f"dfnew \n{dfnew}")
#
####### create CSV file and delete if already exists
output_csv = str(outputPath) + ".csv"
if os.path.exists(output_csv):
    arcpy.AddMessage("csv esists")
    os.remove(output_csv)
######
####### output CSV
dfnew.to_csv(output_csv)
##
