#include "tantivy_search.h"
#include "tantivy_search_cxx.h"
#include <filesystem>
#include <iostream>
#include <tantivy_search.h>
#include <utils.h>

namespace fs = std::filesystem;

using namespace Utils;
using namespace std;

// -- Tokenizer configuration
// -- {
// --   "col1": {
// --     "tokenizer": {
// --       "type": "stem",
// --       "stop_word_filters": [
// --         "english",
// --         "french"
// --       ],
// --       "stem_languages": [
// --         "german",
// --         "english"
// --       ],
// --       "length_limit": 60
// --     }
// --   },
// --   "col2": {
// --     "tokenizer": {
// --       "type": "simple"
// --     }
// --   }
// -- }
const std::string stem_json =
    "{\"col1\": {\"tokenizer\": {\"type\": \"stem\", \"stop_word_filters\": "
    "[\"english\", \"french\"], \"stem_languages\": [\"german\", \"english\"], "
    "\"length_limit\": 60}}, \"col2\": {\"tokenizer\": {\"type\": "
    "\"simple\"}}}";

const std::string raw_json =
    "{ \"col1\": { \"tokenizer\": { \"type\": \"raw\" } }, \"col2\": { "
    "\"tokenizer\": {\"type\": \"raw\"} }, \"col3\": { \"tokenizer\": "
    "{\"type\": \"raw\"} } }";

const std::string simple_json =
    "{ \"mapKeys(col1)\": { \"tokenizer\": { \"type\": \"stem\", "
    "\"stop_word_filters\": [\"english\"], \"stem_languages\": [\"english\"]} "
    "}, \"col2\": { \"tokenizer\": {\"type\": \"simple\"} } }";

const std::string simple_json2 =
    "{ \"col1\": { \"tokenizer\": { \"type\": \"stem\", \"stop_word_filters\": "
    "[\"english\"], \"stem_languages\": [\"english\"]} }, \"col2\": { "
    "\"tokenizer\": {\"type\": \"simple\"} } }";

const std::string chinese_json =
    "{\"text\":{\"tokenizer\":{\"type\":\"chinese\"}}}";

void test_default_create() {
  fs::remove_all("./temp");
  tantivy_search_log4rs_initialize("./log", "info", true, false, false);

  std::string index_path{"./temp"};
  std::vector<std::string> column_names;
  column_names.push_back("text");
  ffi_create_index(index_path, column_names);

  ffi_index_multi_column_docs(
      index_path, 0, {"text"},
      {"Ancient empires rise and fall, shaping history's course."});

  ffi_index_multi_column_docs(
      index_path, 1, {"text"},
      {"Artistic expressions reflect diverse cultural heritages."});
  ffi_index_multi_column_docs(
      index_path, 2, {"text"},
      {"Social movements transform societies, forging new paths."});
  ffi_index_multi_column_docs(index_path, 3, {"text"},
                              {"Economies fluctuate, reflecting the complex "
                               "interplay of global forces."});
  ffi_index_multi_column_docs(
      index_path, 4, {"text"},
      {"Strategic military campaigns alter the balance of power."});
  ffi_index_multi_column_docs(
      index_path, 5, {"text"},
      {"Quantum leaps redefine understanding of physical laws."});
  ffi_index_multi_column_docs(
      index_path, 6, {"text"},
      {"Chemical reactions unlock mysteries of nature."});
  ffi_index_multi_column_docs(
      index_path, 7, {"text"},
      {"Philosophical debates ponder the essence of existence."});
  ffi_index_multi_column_docs(
      index_path, 8, {"text"},
      {"Marriages blend traditions, celebrating love's union."});
  ffi_index_multi_column_docs(
      index_path, 9, {"text"},
      {"Explorers discover uncharted territories, expanding world maps."});
  ffi_index_multi_column_docs(
      index_path, 10, {"text"},
      {"Innovations in technology drive societal progress."});
  ffi_index_multi_column_docs(
      index_path, 11, {"text"},
      {"Environmental conservation efforts protect Earth's biodiversity."});
  ffi_index_multi_column_docs(
      index_path, 12, {"text"},
      {"Diplomatic negotiations seek to resolve international conflicts."});
  ffi_index_multi_column_docs(
      index_path, 13, {"text"},
      {"Ancient philosophies provide wisdom for modern dilemmas."});
  ffi_index_multi_column_docs(
      index_path, 14, {"text"},
      {"Economic theories debate the merits of market systems."});
  ffi_index_multi_column_docs(
      index_path, 15, {"text"},
      {"Military strategies evolve with technological advancements."});
  ffi_index_multi_column_docs(
      index_path, 16, {"text"},
      {"Physics theories delve into the universe's mysteries."});
  ffi_index_multi_column_docs(
      index_path, 17, {"text"},
      {"Chemical compounds play crucial roles in medical breakthroughs."});
  ffi_index_multi_column_docs(
      index_path, 18, {"text"},
      {"Philosophers debate ethics in the age of artificial intelligence."});
  ffi_index_multi_column_docs(
      index_path, 19, {"text"},
      {"Wedding ceremonies across cultures symbolize lifelong commitment."});
  ffi_index_writer_commit(index_path);

  ffi_load_index_reader(index_path);

  auto result = ffi_bm25_search(index_path, "of", 10, {}, false);

  for (auto it : result) {
    cout << "rowid:" << it.row_id << " score:" << it.score
         << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << endl;
  }

  ffi_free_index_reader(index_path);
  ffi_free_index_writer(index_path);

  cout << __func__ << " done" << endl;
}

void test_tokenizer_create() {
  fs::remove_all("./temp");
  tantivy_search_log4rs_initialize("./log", "info", true, false, false);

  std::string index_path{"./temp"};
  std::vector<std::string> column_names;
  column_names.push_back("text");
  ffi_create_index_with_parameter(index_path, column_names, chinese_json);

  ffi_index_multi_column_docs(index_path, 0, {"text"},
                              {"古代帝国的兴衰更迭，不仅塑造了历史的进程，也铭"
                               "刻了时代的变迁与文明的发展。"});
  ffi_index_multi_column_docs(index_path, 1, {"text"},
                              {"艺术的多样表达方式反映了不同文化的丰富遗产，展"
                               "现了人类创造力的无限可能。"});
  ffi_index_multi_column_docs(
      index_path, 2, {"text"},
      {"社会运动如同时代的浪潮，改变着社会的面貌，为历史开辟新的道路和方向。"});
  ffi_index_multi_column_docs(index_path, 3, {"text"},
                              {"全球经济的波动复杂多变，如同镜子反映出世界各国"
                               "之间错综复杂的力量关系。"});
  ffi_index_multi_column_docs(
      index_path, 4, {"text"},
      {"战略性的军事行动改变了世界的权力格局，也重新定义了国际政治的均势。"});
  ffi_index_multi_column_docs(
      index_path, 5, {"text"},
      {"量子物理的飞跃性进展，彻底改写了我们对物理世界规律的理解和认知。"});
  ffi_index_multi_column_docs(
      index_path, 6, {"text"},
      {"化学反应不仅揭开了大自然奥秘的一角，也为科学的探索提供了新的窗口。"});
  ffi_index_multi_column_docs(
      index_path, 7, {"text"},
      {"哲学家的辩论深入探讨了生命存在的本质，引发人们对生存意义的深刻思考。"});
  ffi_index_multi_column_docs(index_path, 8, {"text"},
                              {"婚姻的融合不仅是情感的结合，更是不同传统和文化"
                               "的交汇，彰显了爱的力量。"});
  ffi_index_multi_column_docs(
      index_path, 9, {"text"},
      {"勇敢的探险家发现了未知的领域，为人类的世界观增添了新的地理篇章。"});
  ffi_index_multi_column_docs(
      index_path, 10, {"text"},
      {"科技创新的步伐从未停歇，它推动着社会的进步，引领着时代的前行。"});
  ffi_index_multi_column_docs(index_path, 11, {"text"},
                              {"环保行动积极努力保护地球的生物多样性，为我们共"
                               "同的家园筑起绿色的屏障。"});
  ffi_index_multi_column_docs(
      index_path, 12, {"text"},
      {"外交谈判在国际舞台上寻求和平解决冲突，致力于构建一个更加和谐的世界。"});
  ffi_index_multi_column_docs(
      index_path, 13, {"text"},
      {"古代哲学的智慧至今仍对现代社会的诸多难题提供启示和解答，影响深远。"});
  ffi_index_multi_column_docs(index_path, 14, {"text"},
                              {"经济学理论围绕市场体系的优劣进行了深入的探讨与"
                               "辩论，对经济发展有重要指导意义。"});
  ffi_index_multi_column_docs(
      index_path, 15, {"text"},
      {"随着科技的不断进步，军事战略也在不断演变，应对新时代的挑战和需求。"});
  ffi_index_multi_column_docs(
      index_path, 16, {"text"},
      {"现代物理学理论深入挖掘宇宙的奥秘，试图解开那些探索宇宙时的未知之谜。"});
  ffi_index_multi_column_docs(index_path, 17, {"text"},
                              {"在医学领域，化学化合物的作用至关重要，它们在许"
                               "多重大医疗突破中扮演了核心角色。"});
  ffi_index_multi_column_docs(index_path, 18, {"text"},
                              {"当代哲学家在探讨人工智能时代的伦理道德问题，对"
                               "机器与人类的关系进行深刻反思。"});
  ffi_index_multi_column_docs(index_path, 19, {"text"},
                              {"不同文化背景下的婚礼仪式代表着一生的承诺与责任"
                               "，象征着两颗心的永恒结合。"});

  ffi_index_writer_commit(index_path);

  ffi_load_index_reader(index_path);

  auto result = ffi_bm25_search(index_path, "影响深远", 10, {}, false);

  for (auto it : result) {
    cout << "rowid:" << it.row_id << " score:" << it.score
         << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << endl;
  }

  ffi_free_index_reader(index_path);
  ffi_free_index_writer(index_path);

  cout << __func__ << " done" << endl;
}

int main() {
  test_default_create();
  test_tokenizer_create();
  return 0;
}
