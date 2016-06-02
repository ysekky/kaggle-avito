#convert time to an integer (seconds from min(time))
# データセットから時間を取得
time2 <- strptime(s01_search$SearchDate, '%Y-%m-%d %H:%M:%S')
# 最小値を取得
min_time2 <- min(time2)
# 最小値を日付ベースにする
min_time3 <- as.Date(min_time2)
# 日付ベースの最小値から何秒たったかをintで保持
time3 <- as.integer(difftime(time2, min_time3, unit='sec'))
# conserve_ramはfullデータの時に実施してる
if(conserve_ram) {
  rm(time2)
}
gc()
# search_time3として何秒立ったかを入れてる
s01_search[, search_time3 := time3]
# "%/%”=>整数除算 %%=>あまり
# round(as.integer(search_time3) %/% 3600)  # 何時間たったか
# “%/%”なのになんでroundしてんの
# ってかもともとas.integerしてるじゃんなんで改めてしてるんの
# as.integer(round(as.integer(search_time3) %/% 3600)) %% 24 # 何時か?
s01_search[, hour := as.integer(round(as.integer(search_time3) %/% 3600)) %% 24]
# 86400 = 3600 * 24
# 何曜日か?
s01_search[, dow := as.integer(round(as.integer(search_time3) %/% 86400)) %% 7]
# nchar => length
# クエリの長さ
s01_search[, query_nchar := nchar(SearchQuery)]
# パラメータの長さ
s01_search[, param_nchar := nchar(SearchParams)]

# クエリ入れてる率
user_search_type_sum2 <- s01_search[, .(user_query_ratio=mean(SearchQuery!='')), by=UserID]
# user_idでサマった平均のクエリの長さ, パラメータの長さの平均
user_search_query_sum <- s01_search[, .(user_search_nchar_mean=mean(query_nchar), user_search_param_nchar_mean=mean(param_nchar)), by=UserID]

# ユーザのLocationのエントロピー
user_search_loc_entropy <- calc_entropy(s01_search, 'UserID', 'LocationID', 'user_search_loc')
# 曜日のエントロピー,
user_search_dow_entropy <- calc_entropy(s01_search, 'UserID', 'dow', 'user_search_dow')
# hourのエントロピー,
user_search_hour_entropy <- calc_entropy(s01_search, 'UserID', 'hour', 'user_search_hour')
# search paramsのエントロピー?
user_sparam_entropy <- calc_entropy(s01_search, 'UserID', 'SearchParams', 'user_search_sparam')

# oneに1を代入
s01_search[, one := 1]
# user_idと時間でソート
s01_search <- s01_search[order(UserID, -search_time3), ]
# ユーザIDのリストで1を累積和
# 何回目の検索かを記録してる
a <- s01_search[, cumsum(one), by=list(UserID)]
# search_time_rseq
s01_search[, search_time_rseq := a$V1]

# rsecとsec
# orderを逆順に
s01_search <- s01_search[order(UserID, search_time3), ]
a <- s01_search[, cumsum(one), by=list(UserID)]
s01_search[, search_time_seq := a$V1]

# 前の検索時間をいれる
tmp <- c(NA, head(s01_search$search_time3, -1))
s01_search[, prev_search_time3:=tmp]

# 前のユーザID入れてる
# よくわかんない、最初とか最後を定義したいのかなと考えてる
tmp <- c(NA, head(s01_search$UserID, -1))
s01_search[, prev_user_id:=tmp]

# 前の検索
s01_search[, prev_search_query:=c(NA, head(SearchQuery, -1))]
s01_search[, prev_search_param:=c(NA, head(SearchParams, -1))]
# 前の検索からの経過時間
s01_search[, time_gap_prev_search := search_time3 - prev_search_time3]
# 前のユーザIDがないか違ったら経過時間を-1いれる
s01_search[is.na(prev_user_id) | (UserID != prev_user_id), time_gap_prev_search := (-1)]
# is_gap, 連続した検索か? ユーザが変わるか900秒経つかで設定してる
s01_search[, is_gap := as.numeric(time_gap_prev_search < 0 | time_gap_prev_search > 900)]
# sessionのカウント. 何個目のセッションか
tmp <- s01_search[, cumsum(is_gap), by=list(UserID)]
s01_search[, user_session_no := tmp$V1]
# セッション内での検索数
tmp <- s01_search[, cumsum(one), by=list(UserID, user_session_no)]
s01_search[, user_session_seq := tmp$V1]
#
s01_search[, new_search := as.numeric(is.na(prev_user_id) | UserID != prev_user_id | SearchQuery != prev_search_query |　SearchParams != prev_search_param)]

tmp <- s01_search[, cumsum(new_search), by=list(UserID)]
s01_search[, user_search_no := tmp$V1]
tmp <- s01_search[, cumsum(one), by=list(UserID, user_search_no)]
s01_search[, user_same_search_seq := tmp$V1]

user_search_nzquery_cnt <- s01_search[, sum(SearchQuery != ''), by=list(UserID)]
setnames(user_search_nzquery_cnt, 'V1', 'user_search_nzquery_cnt')
user_search_cnt <- s01_search[, .N, by=list(UserID)]
setnames(user_search_cnt, 'N', 'user_search_cnt')
left_merge_inplace(user_search_cnt, user_search_nzquery_cnt, by='UserID')

ad_visit_cnt <- s01_visit[, .N, by=list(AdID)]
setnames(ad_visit_cnt, 'N', 'ad_visit_cnt')
left_merge_inplace(s01_ths, ad_visit_cnt, by='AdID', verbose=T)
s01_ths[is.na(ad_visit_cnt), ad_visit_cnt := 0]

search_ad_cnt <- s01_ths[, list(.N, sum(ad_visit_cnt)), by=list(SearchID)]
setnames(search_ad_cnt, c('N', 'V2'), c('search_ad_cnt', 'total_other_ads_visit_cnt'))

search_ad1_cnt <- s01_ths[ObjectType==1, .N, by=list(SearchID)]
setnames(search_ad1_cnt, 'N', 'search_ad1_cnt')
search_ad2_cnt <- s01_ths[ObjectType==2, .N, by=list(SearchID)]
setnames(search_ad2_cnt, 'N', 'search_ad2_cnt')
left_merge_inplace(search_ad_cnt, search_ad1_cnt, by='SearchID')
left_merge_inplace(search_ad_cnt, search_ad2_cnt, by='SearchID')
dt_fill_na(search_ad_cnt)
search_ad_cnt[, search_ad3_cnt := search_ad_cnt - search_ad1_cnt - search_ad2_cnt]

gc()
