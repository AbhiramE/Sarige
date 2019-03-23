import datetime
import os

import pandas as pd
import yaml
from flask import Flask, request

app = Flask(__name__)


@app.before_first_request
def create_data_files():
    # Load config file.
    global config, data_file_name
    with open("../config.yml", 'r') as stream:
        try:
            config = yaml.load(stream)
        except yaml.YAMLError as exc:
            print("Could not load config file", exc)

    # Check if data file exists. Else create one
    data_file_name = config['data']['data_file_prefix']
    # ToDo: Change file name suffix based on period of updates.
    data_file_name += str(datetime.datetime.now().strftime("%Y-%m")) + ".csv"
    dirname = os.path.dirname(__file__)
    file_name = os.path.join(dirname, data_file_name)

    if not os.path.isfile(file_name):
        df = pd.DataFrame(columns=['route_id', 'block_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
                                   'saturday', 'sunday'])
        df = prepopulate_routes_and_blocks(df)
        df = df[df.columns.values].astype(str)
        df.to_csv(data_file_name, index=False)


def prepopulate_routes_and_blocks(df):
    # ToDo: Prepopulate this with fixed routes and blocks. Multiple drives to be handled here.
    for i in range(5):
        df.loc[i] = [str(i), str(i), None, None, None, None, None, None, None]
    return df


@app.route("/relieve")
def assign_relieving_crew():
    block_id = request.args.get("block_id")
    crew_id = request.args.get("crew_id")

    df = pd.read_csv(data_file_name)
    df = df[df.columns.values].astype(str)

    days_available = df.loc[(df['block_id'] == block_id)].isin(['None']).any().drop(labels=['route_id', 'block_id'])
    if not days_available.any():
        return "This block is taken. Try a different block"
    else:
        block_df = df.loc[(df['block_id'] == block_id)].isin(['None'])
        block_df = block_df.apply(lambda x: block_df.columns[x], axis=1)
        for i in block_df.index.values:
            df.loc[i, block_df[i]] = crew_id

        df.to_csv(data_file_name, index=False)
        return df.loc[(df['block_id'] == block_id)].to_json(orient='records')


@app.route("/assign")
def assign_main_crew():
    """
    Function to assign primary drivers
    :return:
    """
    route_id = request.args.get("route_id")
    crew_id = request.args.get("crew_id")
    day_off = request.args.get("day_off").lower()

    columns = config['app']['days'].copy()
    columns.remove(day_off)

    df = pd.read_csv(data_file_name)
    df = df[df.columns.values].astype(str)

    available_days = df.loc[(df['route_id'] == route_id), columns]
    if columns != available_days.columns[available_days.isin(['None']).any()].tolist():
        # Route is not available if any of the days are taken.
        return "This route is taken. Try a different route"
    else:
        df.loc[(df['route_id'] == route_id), columns] = crew_id
        df.to_csv(data_file_name, index=False)
        return "Success!"


@app.route("/")
def hello():
    return "Hello World!"


if __name__ == '__main__':
    app.run(port=8080, debug=True)
