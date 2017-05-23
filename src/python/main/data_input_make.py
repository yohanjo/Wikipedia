'''
Generates input files for LDA and HSTTM.

@author: Yohan Jo
@version: May 23, 2017
'''
from ydata.csv_utils import *
from collections import defaultdict
import re, sys

csv.field_size_limit(sys.maxsize)

in_dir = "/Users/Yohan/Dropbox/Research/Wikipedia/wikipedia/data"
out_dir = "/Users/Yohan/Dropbox/Research/Wikipedia/data/HSTTM"
talk_path = in_dir+"/talk/enwiki/talk_filtered_2.csv"
content_path = in_dir+"/latest_revisions_100k.csv"
talk_out_path = out_dir+"/talk.csv"
content_out_path = out_dir+"/content.csv"
talk_content_map_path = out_dir+"/talk_content.csv"
stats_path = out_dir+"/stats.txt"

# Read talk pages
print "Reading talk pages..."
talk = defaultdict(lambda: defaultdict(list))
id_set = set()
for row in iter_csv_header(talk_path):
    itemid = (row["article"],row["thread"],row["username"],row["timestamp"])
    if itemid in id_set: continue
    talk[row["article"]][row["thread"]].append(
        (int(row['contribution_id']), row["timestamp"], row["username"],
        re.sub("\\s+", " ", row["text"])))
    id_set.add(itemid)
talk_articles = set(talk.keys())


# Read content pages
print "Reading content pages..."
content = dict()
for row in iter_csv_noheader(content_path):
    docid = row[0]
    if docid not in talk: continue
    content[docid] = re.sub("\\s+", " ", row[2])
content_articles = set(content.keys())
 
common_articles = talk_articles & content_articles

# Print HSTTM input
with open(talk_out_path, "w") as out_file:
    out_csv = csv.writer(out_file)
    out_csv.writerow(["SeqId", "InstNo", "Author", "Parent", "Domain", "Text"])
    for article in sorted(common_articles):
        for thread_title, thread in talk[article].iteritems():
            seq_id = article + "###" + thread_title
            thread_starter = None
            num_colons = []
            index = 0
            for cid, dtype, user, text in sorted(thread):
                colons = len(re.search("^:*", text).group())
                if index==0 or dtype=="THREAD_STARTER": 
                    parent = -1
                    thread_starter = index
                else:
                    for i in reversed(xrange(index)):
                        if i == thread_starter \
                                or (colons == 0 and num_colons[i] == 0) \
                                or (colons > 0 and num_colons[i] < colons): 
                            parent = i
                            break
                    assert i >= 0
                out_csv.writerow([seq_id, index, user, parent, article, text])
                num_colons.append(colons)
                index += 1
            

# Print LDA input
with open(content_out_path, "w") as out_file:
    out_csv = csv.writer(out_file)
    out_csv.writerow(["DocId", "Text"])
    for article in sorted(common_articles):
        out_csv.writerow([article, content[article]])

# Print talk-content mapping
# with open(talk_content_map_path, "w") as out_file:
#     out_csv = csv.writer(out_file)
#     out_csv.writerow(["SeqId", "DocId"])
#     for article in sorted(common_articles):
#         for thread_title, thread in talk[article].iteritems():
#             seq_id = article + "#" + thread_title
#             out_csv.writerow([seq_id, article])

with open(stats_path,"w") as out_file:
    print>>out_file, "Common articles:", len(common_articles)
    print>>out_file, " - Content only:", len(content_articles - talk_articles)
    print>>out_file, " - Talk only:", len(talk_articles - content_articles)
    for article in talk_articles - content_articles: print>>out_file, article
    print>>out_file, "Total num of threads:", \
                len(set([(article,thread) for article,thread,_,_ in id_set]))
    print>>out_file, "Num of threads included:", \
                len(set([(article,thread) for article,thread,_,_ in id_set 
                                          if article in common_articles]))
