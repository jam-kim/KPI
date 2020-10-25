import argparse
import pandas as pd
import numpy as np
from pyarrow import csv
import pyarrow.parquet as pq
import time
import os
import copy
import json
from pandas.api.types import is_numeric_dtype


parser = argparse.ArgumentParser()
parser.add_argument("--directory_load", "-ld", help="저장된 경로를 작성해주세요")
parser.add_argument("--directory_save", "-sd", help="결과 파일 저장 경로를 작성해주세요") 
parser.add_argument("--name", "-n", help="파일 이름을 작성해주세요")
args = parser.parse_args()


class dqc_code:
    def __init__(self):
        try:
            os.chdir(args.directory_load)
            if ".csv" in args.name:
                self.data = pd.read_csv(args.name)
            elif ".parquet" in args.name:
                self.data = pq.read_pandas(args.name).to_pandas()
            self.nalists = ["?", "na", "na", "null", "Null", "NULL", " "]
            self.output = dict()
        except:
            print("해당 파일이 존재하지 않습니다. 경로를 확인하세요.")

    def na_check(self):
        # 공통 결측치
        addlist1 = input(
            f"전체 공통 결측값 변경 후 개별 변수 결측값을 변경합니다. /n기본 설정된 공통 결측값 리스트: {self.nalists} \n추가하고 싶은 리스트를 작성해주십시오:"
        )
        if len(addlist1) != 0:
            self.nalists += addlist1.split(sep=", ")
            self.nalists = list(set(self.nalists))
        # change all NA
        self.data[self.data.isin(self.nalists)] = np.nan

        # 각 변수별 결측치
        for col in self.data.columns:
            addlist2 = input(f"{col} 변수에서 추가 변경하고 싶은 리스트를 작성해주십시오:")
            if len(addlist2) != 0:
                addlist2.split(sep=", ")
                addlist2 = list(set(addlist2))
                self.data[self.data.loc[:, col].isin(addlist2)] = np.nan

    def datainfo(self):
        # data 크기 저장.
        rows, columns = self.data.shape

        # data 내 전체 결측값을 확인한다.
        total_null = sum(self.data.isnull().sum())

        # 중복되는 row가 있는지 확인한다.
        duplicate_row = sum(self.data.duplicated())

        # 중복 row의 index를 확인한다.
        duplicate_index = [
            idx
            for idx, result in self.data.duplicated().to_dict().items()
            if result is True
        ]

        ## 연속형 변수 리스트
        numeric_var = list(self.data.select_dtypes(include=[np.number]).columns)

        ## 범주형 변수 리스트
        string_var = list(self.data.select_dtypes(include=[np.object]).columns)

        self.output["dataset"] = {
            "rows": rows,
            "cols": columns,
            "null": total_null,
            "numeric_var": numeric_var,
            "sting_var": string_var,
            "duplicate row": duplicate_row,
            "duplicate row index": duplicate_index,
        }

    def dqc(self):
        # 변수별 summary.
        variables = dict()
        string_var = dict()
        numeric_var = dict()

        for column_name in self.data.columns.to_list():

            temp_dict = dict()

            # 값 전체가 결측값인 column은 all_null 값이 1로 입력된다.
            if self.data.shape[0] == self.data[column_name].isnull().sum():
                temp_dict["all_null"] = 1
            else:
                temp_dict["all_null"] = 0

            # column 내에 결측값 갯수를 확인한다.
            temp_dict["null_count"] = int(self.data[column_name].isnull().sum())

            # 값 전체가 동일한 column은 all_same 값이 1로 입력된다.
            if len(self.data[column_name].unique()) == 1:
                temp_dict["all_same"] = 1
            else:
                temp_dict["all_same"] = 0

            #             # 중복되는 column일 경우 duplicate 값이 1로 입력된다.
            #             if column_name in duplicate_column:
            #                 temp_dict['duplicate'] = 1
            #             else:
            #                 temp_dict['duplicate'] = 0

            # is_numeric_dtype 함수를 가지고 column의 string, numeric type을 구분한다.
            if is_numeric_dtype(self.data[column_name]) is True:
                temp_describe = dict(self.data[column_name].describe())
                for i in temp_describe.keys():
                    temp_describe[i] = float(temp_describe[i])
                temp_dict.update(temp_describe)
                numeric_var[column_name] = temp_dict
            else:
                values = self.data[column_name].astype(str)
                values_percent = round(
                    values.groupby(values).count().sort_values(ascending=False)
                    / len(values),
                    2,
                )
                temp_dict["values_percent"] = values_percent.to_dict()
                string_var[column_name] = temp_dict

            self.output["variable"] = {
                "numeric_var": numeric_var,
                "string_var": string_var,
            }

        # dqc table 출력하기
        column = [
            ["컬럼"] * 3 + ["연속형 대상"] * 4 + ["범주형 대상"] * 4 + ["공통"] * 6,
            [
                "컬럼명",
                "컬럼 설명",
                "타입",
                "최소값",
                "최대값",
                "평균",
                "표준편차",
                "범주 수",
                "범주",
                "범주%",
                "정의된 범주 외",
                "최빈값",
                "NULL값",
                "NULL수",
                "NULL%",
                "적재건수",
                "적재건수%",
            ],
        ]
        self.result = pd.DataFrame(columns=column)

        for ctype in self.output["variable"].keys():
            for cname in self.output["variable"][ctype].keys():
                datainfo = self.output["dataset"]
                datasummary = self.output["variable"][ctype][cname]
                if ctype == "numeric_var":
                    temp_df = pd.DataFrame(
                        np.array(
                            (
                                cname,
                                ctype,
                                round(datasummary["min"], 2),
                                round(datasummary["max"], 2),
                                round(datasummary["mean"], 2),
                                round(datasummary["std"], 2),
                                datasummary["null_count"],
                                round(
                                    datasummary["null_count"] / datainfo["rows"] * 100,
                                    2,
                                ),
                                datainfo["rows"] - datasummary["null_count"],
                                100
                                - round(
                                    datasummary["null_count"] / datainfo["rows"] * 100,
                                    2,
                                ),
                            )
                        ).reshape(1, 10),
                        columns=[
                            ["컬럼"] * 2 + ["연속형 대상"] * 4 + ["공통"] * 4,
                            [
                                "컬럼명",
                                "타입",
                                "최소값",
                                "최대값",
                                "평균",
                                "표준편차",
                                "NULL수",
                                "NULL%",
                                "적재건수",
                                "적재건수%",
                            ],
                        ],
                    )
                else:
                    str_values = self.output["variable"][ctype][cname]
                    datacount = datasummary["values_percent"]
                    temp_df = pd.DataFrame(
                        np.array(
                            (
                                cname,
                                ctype,
                                len(datacount),
                                ", ".join(list(datacount.keys())),
                                str(datacount).replace("{", "").replace("}", ""),
                                datasummary["null_count"],
                                round(
                                    datasummary["null_count"] / datainfo["rows"] * 100,
                                    2,
                                ),
                                datainfo["rows"] - datasummary["null_count"],
                                100
                                - round(
                                    datasummary["null_count"] / datainfo["rows"] * 100,
                                    2,
                                ),
                            )
                        ).reshape(1, 9),
                        columns=[
                            ["컬럼"] * 2 + ["범주형 대상"] * 3 + ["공통"] * 4,
                            [
                                "컬럼명",
                                "타입",
                                "범주 수",
                                "범주",
                                "범주%",
                                "NULL수",
                                "NULL%",
                                "적재건수",
                                "적재건수%",
                            ],
                        ],
                    )
                    # 정의된 범주 외 NULL값을 추가해야될 거 같은데..

                self.result = pd.concat(
                    [self.result, temp_df], ignore_index=True
                ).reindex(columns=column)

    def save(self):
        os.chdir(args.directory_save)
        json.dump(self.output, open("datainfo.json", "w"))
        self.result.to_excel("dqctable.xlsx")
        print("저장완료")

test = dqc_code()
test.datainfo()
test.dqc()
test.save()

