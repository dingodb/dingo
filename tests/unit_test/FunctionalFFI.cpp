#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <tantivy_search.h>
#include <utils.h>
#include <filesystem>

namespace fs = std::filesystem;

using namespace Utils;
using namespace std;
using namespace rust::cxxbridge1;

class FunctionalFFITest : public ::testing::Test {
protected:
    const string indexDirectory = "./temp";
    const string logPath = "./log";
    const vector<string> column_names = {"col1", "col2"};

    void SetUp(){
        ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "trace", true, false, false));
    }
    void TearDown(){
        ASSERT_NO_THROW(ffi_free_index_writer(indexDirectory));
        ASSERT_NO_THROW(ffi_free_index_reader(indexDirectory));
        fs::remove_all(indexDirectory);
    }

    void indexSomeChineseDocs(const string& chineseJosnTokenizerParameter){
        ASSERT_TRUE(ffi_create_index_with_parameter(indexDirectory, column_names, chineseJosnTokenizerParameter));

        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 0, column_names, {"古代帝国的兴衰更迭，不仅塑造了历史的进程，也铭刻了时代的变迁与文明的发展。", "艺术的多样表达方式反映了不同文化的丰富遗产，展现了人类创造🦠力的无限可能。"}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 1, column_names, {"社会运动如同时代的浪潮，改变着社会的面貌，为历史开辟新的道路和方向。", "全球经济的波动复杂多变🦠，如同镜子反映出世界各国之间错综复杂的力量关系。"}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 2, column_names, {"战略性的军事行动改变了世界的权力格局，也重新定义了国际政治的均势。", "量子物理的飞跃性进展，彻底改写了我们对物理世界规律的理解和认知。"}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 3, column_names, {"化学反应不仅揭开了大自然奥秘的一角，也为科学的探索提供了新的窗口。", "哲学家的辩论深入探讨了生命存在的本质，引发人们对生存意义的深刻思考。"}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 4, column_names, {"婚姻的融合不仅是情感的结合，更是不同传统和文化的交汇，彰显了爱的力量。", "勇敢的探险家发现了未知的领域，为人类的世界观增添了新的地理篇章。"}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 5, column_names, {"科技创新的步伐从未停歇，🦠 它推动着社会的进步，引领着时代的前行。", "环保行动积极努力保护地球的生物多样性，为我们共同的家园筑起绿色的屏障。"}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 6, column_names, {"外交谈判在国际舞台上寻求和平解决冲突，致力于构建一个更加和谐的世界。", "古代哲学的智慧至今仍对现代社会的诸多难题提供启示和解答，影响深远。"}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 7, column_names, {"经济学理论围绕市场体系的优劣进行了深入的探讨与辩论，对经济发展有重要指导意义。", "随着科技的不断进步，军事战略也在不断演变，应对新时代的挑战和需求。"}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 8, column_names, {"现代物理学理论深入挖掘宇宙的奥秘，试图解开那些探索宇宙时的未知之谜。", "在医学领域，化学化合物的作用至关重要，它们在许多重大医疗突破中扮演了核心角色。"}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 9, column_names, {"当代哲学家在探讨人工智能时代的伦理道德问题，对机器与人类的关系进行深刻反思。", "不同文化背景下的婚礼仪式代表着一生的承诺与责任，象征着两颗心的永恒结合。"}));

        ASSERT_TRUE(ffi_index_writer_commit(indexDirectory));
        ASSERT_TRUE(ffi_load_index_reader(indexDirectory));
    }

    void indexSomeEnglishDocs(const string& englishJosnTokenizerParameter){
        ASSERT_TRUE(ffi_create_index_with_parameter(indexDirectory, column_names, englishJosnTokenizerParameter));

        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 0, column_names, {"Ancient empires rise and fall, shaping history's course.", "Artistic expressions reflect diverse cultural heritages."}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 1, column_names, {"Social movements transform societies, forging new paths.", "Economies fluctuate🦠, reflecting the complex interplay of global forces."}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 2, column_names, {"Strategic military campaigns alter the balance of power.", "Quantum leaps redefine understanding of physical laws."}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 3, column_names, {"Chemical reactions unlock mysteries of nature.", "Philosophical debates ponder the essence of existence.🦠"}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 4, column_names, {"Marriages blend traditions, celebrating love's union.", "Explorers discover uncharted territories, expanding world maps."}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 5, column_names, {"Innovations 🦠 in technology drive societal progress.", "Environmental conservation efforts protect Earth's biodiversity."}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 6, column_names, {"Diplomatic negotiations seek to resolve international conflicts.", "Ancient philosophies provide wisdom for modern dilemmas."}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 7, column_names, {"Economic theories debate the merits of market systems.", "Military strategies evolve with technological advancements."}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 8, column_names, {"Physics theories delve into the universe's mysteries.", "Chemical compounds play crucial roles in medical breakthroughs."}));
        ASSERT_TRUE(ffi_index_multi_column_docs(indexDirectory, 9, column_names, {"Philosophers debate ethics in the age of artificial intelligence.", "Wedding ceremonies across cultures symbo🦠lize lifelong commitment."}));

        ASSERT_TRUE(ffi_index_writer_commit(indexDirectory));
        ASSERT_TRUE(ffi_load_index_reader(indexDirectory));
    }
};

TEST_F(FunctionalFFITest, TantivyDeleteRowIds) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"stem\"}}, \"col2\":{\"tokenizer\":{\"type\":\"stem\"}}}");
        Vec<RowIdWithScore> beforeDeleteTerm = ffi_bm25_search(indexDirectory, "Ancient", 10, {}, false);
        ASSERT_TRUE(beforeDeleteTerm.size()==2);
        ASSERT_TRUE(ffi_delete_row_ids(indexDirectory, {0, 6, 1000}));
        Vec<RowIdWithScore> afterDeleteTerm = ffi_bm25_search(indexDirectory, "Ancient", 10, {}, false);
        ASSERT_TRUE(afterDeleteTerm.size()==0);
    });
}

TEST_F(FunctionalFFITest, FFIQueryTermWithRange) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"stem\"}}, \"col2\":{\"tokenizer\":{\"type\":\"stem\"}}}");
        ASSERT_TRUE(ffi_query_term_with_range(indexDirectory, column_names[0], "Ancient", 0, 1));
        ASSERT_FALSE(ffi_query_term_with_range(indexDirectory, column_names[0], "Ancient", 3, 9));
        ASSERT_FALSE(ffi_query_term_with_range(indexDirectory, column_names[0], "Ancient", 29, 33));
    });
}

TEST_F(FunctionalFFITest, FFIQueryTermsWithRange) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"stem\"}}, \"col2\":{\"tokenizer\":{\"type\":\"stem\"}}}");
        ASSERT_TRUE(ffi_query_terms_with_range(indexDirectory, column_names[0], {"Ancient", "Social", "Chemical"}, 0, 1));
        ASSERT_FALSE(ffi_query_terms_with_range(indexDirectory, column_names[0], {"Ancient", "Social", "Chemical"}, 2, 2));
        ASSERT_TRUE(ffi_query_terms_with_range(indexDirectory, column_names[1], {"Ancient", "Social", "Chemical"}, 8, 20));
    });
}

TEST_F(FunctionalFFITest, FFIQuerySentenceWithRange) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"stem\"}}, \"col2\":{\"tokenizer\":{\"type\":\"stem\"}}}");
        ASSERT_TRUE(ffi_query_sentence_with_range(indexDirectory, column_names[1], "Military strategies evolve with technological advancements.", 7, 7));
        ASSERT_FALSE(ffi_query_sentence_with_range(indexDirectory, column_names[0], "Military strategies evolve with technological advancements.", 9, 9));
    });
}

TEST_F(FunctionalFFITest, FFIRegexTermWithRange) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"whitespace\"}}, \"col2\":{\"tokenizer\":{\"type\":\"raw\"}}}");
        ASSERT_TRUE(ffi_regex_term_with_range(indexDirectory, column_names[1], "%ate🦠, refl%", 0, 2));
        ASSERT_FALSE(ffi_regex_term_with_range(indexDirectory, column_names[1], "%ate🦠, refl%", 8, 8));
        ASSERT_FALSE(ffi_regex_term_with_range(indexDirectory, column_names[0], "%ns 🦠 in te%", 4, 6));
        ASSERT_TRUE(ffi_regex_term_with_range(indexDirectory, column_names[0], "%heori%", 7, 8));
    });
}




TEST_F(FunctionalFFITest, FFIQueryTermBitmap) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"stem\"}}, \"col2\":{\"tokenizer\":{\"type\":\"stem\"}}}");
        ASSERT_FALSE(ffi_query_term_bitmap(indexDirectory, column_names[0], "Ancient").size()==0);
        ASSERT_FALSE(ffi_query_term_bitmap(indexDirectory, column_names[1], "Ancient").size()==0);
        ASSERT_TRUE(ffi_query_term_bitmap(indexDirectory, column_names[1], "Innovations").size()==0);
    });
}
TEST_F(FunctionalFFITest, FFIQueryTermsBitmap) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"whitespace\"}}, \"col2\":{\"tokenizer\":{\"type\":\"stem\"}}}");
        ASSERT_FALSE(ffi_query_terms_bitmap(indexDirectory, column_names[0], {"Ancient", "Social", "Chemical"}).size()==0);
        ASSERT_FALSE(ffi_query_terms_bitmap(indexDirectory, column_names[1], {"Ancient", "Social", "Chemical"}).size()==0);
        ASSERT_TRUE(ffi_query_terms_bitmap(indexDirectory, column_names[1], {"unlock", "merits", "ethics"}).size()==0);
    });
}

TEST_F(FunctionalFFITest, FFIQuerySentenceBitmap) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"whitespace\"}}, \"col2\":{\"tokenizer\":{\"type\":\"stem\"}}}");
        ASSERT_FALSE(ffi_query_sentence_bitmap(indexDirectory, column_names[1], "Environmental conservation efforts protect Earth's biodiversity.").size()==0);
        ASSERT_TRUE(ffi_query_sentence_bitmap(indexDirectory, column_names[0], "Environmental conservation efforts protect Earth's biodiversity.").size()==0);
    });
}
TEST_F(FunctionalFFITest, FFIRegexTermBitmap) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"whitespace\"}}, \"col2\":{\"tokenizer\":{\"type\":\"raw\"}}}");
        ASSERT_FALSE(ffi_regex_term_bitmap(indexDirectory, column_names[1], "%ate🦠, refl%").size()==0);
        ASSERT_TRUE(ffi_regex_term_bitmap(indexDirectory, column_names[0], "%ate🦠, refl%").size()==0);
        ASSERT_TRUE(ffi_regex_term_bitmap(indexDirectory, column_names[0], "%ns 🦠 in te%").size()==0);
        ASSERT_FALSE(ffi_regex_term_bitmap(indexDirectory, column_names[0], "%heori%").size()==0);
    });
}

TEST_F(FunctionalFFITest, FFIBM25Search) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"stem\"}}, \"col2\":{\"tokenizer\":{\"type\":\"stem\"}}}");
        Vec<RowIdWithScore> result = ffi_bm25_search(indexDirectory, "the of", 3, {}, false);

        ASSERT_TRUE(result[0].row_id == 3);
        ASSERT_TRUE(result[1].row_id == 2);
        ASSERT_TRUE(result[2].row_id == 1);
    });
}



TEST_F(FunctionalFFITest, FFIBM25SearchWithStopWords) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"stem\", \"stop_word_filters\":[\"english\"]}}, \"col2\":{\"tokenizer\":{\"type\":\"stem\", \"stop_word_filters\":[\"english\"]}}}");
        Vec<RowIdWithScore> result = ffi_bm25_search(indexDirectory, "the of abc", 3, {}, false);
        
        ASSERT_TRUE(result.size() == 0);
    });
}


TEST_F(FunctionalFFITest, TantivyBM25SearchWithFilter) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("{\"col1\":{\"tokenizer\":{\"type\":\"stem\", \"stop_word_filters\":[\"english\"]}}, \"col2\":{\"tokenizer\":{\"type\":\"stem\"}}}");
        vector<uint8_t> aliveRowIds;
        aliveRowIds.push_back(6); // 00000110 -> row_id: [1, 2], number: 2+4=6
        aliveRowIds.push_back(1); // 00000001 -> row_id: [8], number: 1
        Vec<RowIdWithScore> result = ffi_bm25_search(indexDirectory, "the of", 10, aliveRowIds, true);

        ASSERT_TRUE(result.size() == 2);
        ASSERT_TRUE(result[0].row_id == 1);
        ASSERT_TRUE(result[1].row_id == 2);
    });
}
