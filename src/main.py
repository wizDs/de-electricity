import pipelines
import argparse

def run_pipeline(config: pipelines.Config) -> None:

    match config.datatype:
        case "spotprice": pipelines.SpotPricePipeline(config).run()
        case "powersystem": pipelines.PowerSystemPipeline(config).run()
        case _: raise Exception("datatype not supported")

def get_args() -> pipelines.Config:

    parser = argparse.ArgumentParser(description='run datapipeline for 1 month')
    parser.add_argument('--year', type=int, help='year for which the data pipeline should be run')
    parser.add_argument('--month', type=int, help='month for which the data pipeline should be run')
    parser.add_argument('--path', default='./data', type=str, help='the destination where data will be persisted')
    parser.add_argument('--datatype', default='spotprice', type=str, help='type of datapipeline to be run', choices=["spotprice", "powersystem"])

    args = parser.parse_args()

    return pipelines.Config(
        year=args.year, 
        month=args.month, 
        path=args.path,
        datatype=args.datatype
    )

if __name__=="__main__":
    config = get_args()
    run_pipeline(config)
