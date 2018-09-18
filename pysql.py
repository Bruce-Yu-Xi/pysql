def update_columns_by_case(self, table, condition_field, update_df):
    columns = list(update_df)
    # edit update_fields
    update_fields = []
    for metric in columns:
        if metric != condition_field:
            update_fields.append("{update_field} = ut.{update_field},".format(update_field=metric))
    update_fields = ("").join(update_fields)
    if update_fields[-1] == ",":
        update_fields = update_fields[:-1]
    df_tuple = []
    for tuple_ in ([item[k] for k in columns] for item in update_df.to_dict('records')):
        df_tuple.append(str(tuple(tuple_))+",")
    df_tuple = ("").join(df_tuple)
    if df_tuple[-1] == ",":
        df_tuple = df_tuple[:-1]
    query = (
    """UPDATE {schema}.{table} as t SET 
     {update_fields} 
     FROM (values
     {df_tuple}) 
     as ut{columns_name} 
     WHERE t.{condition_field} = ut.{condition_field};""".format(schema=self.config['schema'], 
                                    table=table,
                                    update_fields=update_fields,
                                    df_tuple=df_tuple,
                                    columns_name=str(tuple(columns)),
                                  condition_field=condition_field))
    print(query)
    self.cur.execute(query)