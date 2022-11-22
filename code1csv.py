import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser
def get_flag_epv(views):
    if views < 1000:
        return 1
    elif views < 10000:
        return 2
    elif views < 50000:
        return 3
    elif views < 100000:
        return 4
    elif views < 500000:
        return 5
    elif views < 1000000:
        return 6
    elif views < 5000000:
        return 7
    else:
        return 8
def get_flag_epe(eng):
    if eng < 100:
        return 1
    elif eng < 1000:
        return 2
    elif eng < 5000:
        return 3
    elif eng < 10000:
        return 4
    elif eng < 50000:
        return 5
    elif eng < 100000:
        return 6
    elif eng < 1000000:
        return 7
    else:
        return 8
start_time = parser.parse("2022-05-27T00:00:00.000Z") - timedelta(days=7)
end_time = parser.parse("2022-05-27T00:00:00.000Z") - timedelta(days=180)
video = pd.read_csv("/home/digimantra/Downloads/validation_data_video_27052022.csv",lineterminator='\n')
df = video.loc[:, ["channelId", "videoId", "videoPublishedAt", "mediaType", "viewCount", "commentCount", "likeCount"]]
df.dropna(inplace=True)
df["videoPublishedAt"] = df["videoPublishedAt"].apply(lambda x: parser.parse(x))
df = df[df["videoPublishedAt"] < start_time]
df = df[df["videoPublishedAt"] > end_time]
df = df.sort_values(by=['channelId', 'videoPublishedAt'], ascending=False)
df["engagement"] = df["likeCount"] + df["commentCount"]
df_video = df[df["mediaType"] == "video"]
df_video["video_count"] = df_video.groupby('channelId').cumcount()
df_video = df_video[df_video["video_count"] < 15]
df_video["epe_flag"] = df_video["engagement"].apply(get_flag_epe)
df_video["epv_flag"] = df_video["viewCount"].apply(get_flag_epv)
max_epe_bucket = df_video.groupby(['channelId', "epe_flag"]).count()["videoId"].sort_values().groupby(level=0).tail(1).reset_index().rename(columns={"videoId": "epe_max_count"})
max_epv_bucket = df_video.groupby(['channelId', "epv_flag"]).count()["videoId"].sort_values().groupby(level=0).tail(1).reset_index().rename(columns={"videoId": "epv_max_count"})
epe_flag_count = df_video.groupby(['channelId', "epe_flag"]).count()["videoId"].reset_index().rename(columns={"videoId": "epe_flag_count"})
epv_flag_count = df_video.groupby(['channelId', "epv_flag"]).count()["videoId"].reset_index().rename(columns={"videoId": "epv_flag_count"})
df_video = pd.merge(df_video, epe_flag_count, on=["channelId", "epe_flag"])
df_video = pd.merge(df_video, epv_flag_count, on=['channelId', "epv_flag"])
df_video = pd.merge(df_video, max_epe_bucket, on="channelId")
df_video = pd.merge(df_video, max_epv_bucket, on="channelId")
epe_video = df_video[df_video["epe_flag_count"] == df_video["epe_max_count"]].groupby("channelId").mean()["engagement"].reset_index()
epv_video = df_video[df_video["epv_flag_count"] == df_video["epv_max_count"]].groupby("channelId").mean()["viewCount"].reset_index()
df_short = df[df["mediaType"] == "shorts"]
df_short["video_count"] = df_short.groupby('channelId').cumcount()
df_short = df_short[df_short["video_count"] < 15]
df_short["epe_flag"] = df_short["engagement"].apply(get_flag_epe)
df_short["epv_flag"] = df_short["viewCount"].apply(get_flag_epv)
max_epe_bucket = df_short.groupby(['channelId', "epe_flag"]).count()["videoId"].sort_values().groupby(level=0).tail(1).reset_index().rename(columns={"videoId": "epe_max_count"})
max_epv_bucket = df_short.groupby(['channelId', "epv_flag"]).count()["videoId"].sort_values().groupby(level=0).tail(1).reset_index().rename(columns={"videoId": "epv_max_count"})
epe_flag_count = df_short.groupby(['channelId', "epe_flag"]).count()["videoId"].reset_index().rename(columns={"videoId": "epe_flag_count"})
epv_flag_count = df_short.groupby(['channelId', "epv_flag"]).count()["videoId"].reset_index().rename(columns={"videoId": "epv_flag_count"})
df_short = pd.merge(df_short, epe_flag_count, on=['channelId', "epe_flag"])
df_short = pd.merge(df_short, epv_flag_count, on=['channelId', "epv_flag"])
df_short = pd.merge(df_short, max_epe_bucket, on="channelId")
df_short = pd.merge(df_short, max_epv_bucket, on="channelId")
epe_short = df_short[df_short["epe_flag_count"] == df_short["epe_max_count"]].groupby("channelId").mean()["engagement"].reset_index()
epv_short = df_short[df_short["epv_flag_count"] == df_short["epv_max_count"]].groupby("channelId").mean()["viewCount"].reset_index()





calculated_data = pd.read_csv("/home/digimantra/Downloads/validation_data_channels_27052022.csv")
calculated_data = calculated_data.loc[:, ["channel_id", "expectedViewsShorts", "expectedViewsVideo", "expectedEngagementShorts", "expectedEngagementVideo"]]
calculated_data["expectedViewsShorts"] = calculated_data["expectedViewsShorts"].apply(lambda x: float(x.replace(",", "")))
calculated_data["expectedViewsVideo"] = calculated_data["expectedViewsVideo"].apply(lambda x: float(x.replace(",", "")))
calculated_data["expectedEngagementShorts"] = calculated_data["expectedEngagementShorts"].apply(lambda x: float(x.replace(",", "")))
calculated_data["expectedEngagementVideo"] = calculated_data["expectedEngagementVideo"].apply(lambda x: float(x.replace(",", "")))
calculated_data = pd.merge(calculated_data, epe_short, how="left", left_on='channel_id', right_on='channelId').fillna(-1).drop(columns=["channelId"])
calculated_data = pd.merge(calculated_data, epe_video, how="left", left_on='channel_id', right_on='channelId', suffixes=("Shorts", "Video")).fillna(-1).drop(columns=["channelId"])
calculated_data["enagagementShortVariance"] = calculated_data["expectedEngagementShorts"] - calculated_data["engagementShorts"]
calculated_data["enagagementVideoVariance"] = calculated_data["expectedEngagementVideo"] - calculated_data["engagementVideo"]
calculated_data = pd.merge(calculated_data, epv_short, how="left", left_on='channel_id', right_on='channelId').fillna(-1).drop(columns=["channelId"])
calculated_data = pd.merge(calculated_data, epv_video, how="left", left_on='channel_id', right_on='channelId', suffixes=("Shorts", "Video")).fillna(-1).drop(columns=["channelId"])
calculated_data["viewsShortVariance"] = calculated_data["expectedViewsShorts"] - calculated_data["viewCountShorts"]
calculated_data["viewsVideoVariance"] = calculated_data["expectedViewsVideo"] - calculated_data["viewCountVideo"]
calculated_data["viewsShortVariancePercent"] = (calculated_data["viewsShortVariance"] / calculated_data["expectedViewsShorts"]) * 100
calculated_data["viewsVideoVariancePercent"] = (calculated_data["viewsVideoVariance"] / calculated_data["expectedViewsVideo"]) * 100
calculated_data["enagagementShortVariancePercent"] = (calculated_data["enagagementShortVariance"] / calculated_data["expectedEngagementShorts"]) * 100
calculated_data["enagagementVideoVariancePercent"] = (calculated_data["enagagementVideoVariance"] / calculated_data["expectedEngagementVideo"]) * 100
calculated_data.fillna(0, inplace=True)
calculated_data.to_csv("/home/digimantra/Downloads/validation_results.csv", index=False)