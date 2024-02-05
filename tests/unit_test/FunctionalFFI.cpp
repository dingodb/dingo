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
    void SetUp(){
        ASSERT_TRUE(tantivy_search_log4rs_initialize(logPath.c_str(), "trace", true, false, false));
    }
    void TearDown(){
        ASSERT_NO_THROW(tantivy_reader_free(indexDirectory));
        ASSERT_NO_THROW(tantivy_writer_free(indexDirectory));
        fs::remove_all(indexDirectory);
    }
    void indexSomeChineseDocs(const string& chineseTokenizerAndParameter, bool docStore){
        ASSERT_TRUE(tantivy_create_index_with_tokenizer(indexDirectory, chineseTokenizerAndParameter, docStore));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 0, "古代帝国的兴衰更迭，不仅塑造了历史的进程，也铭刻了时代的变迁与文明的发展。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 1, "艺术的多样表达方式反映了不同文化的丰富遗产，展现了人类创造🦠力的无限可能。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 2, "社会运动如同时代的浪潮，改变着社会的面貌，为历史开辟新的道路和方向。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 3, "全球经济的波动复杂多变🦠，如同镜子反映出世界各国之间错综复杂的力量关系。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 4, "战略性的军事行动改变了世界的权力格局，也重新定义了国际政治的均势。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 5, "量子物理的飞跃性进展，彻底改写了我们对物理世界规律的理解和认知。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 6, "化学反应不仅揭开了大自然奥秘的一角，也为科学的探索提供了新的窗口。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 7, "哲学家的辩论深入探讨了生命存在的本质，引发人们对生存意义的深刻思考。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 8, "婚姻的融合不仅是情感的结合，更是不同传统和文化的交汇，彰显了爱的力量。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 9, "勇敢的探险家发现了未知的领域，为人类的世界观增添了新的地理篇章。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 10, "科技创新的步伐从未停歇，🦠 它推动着社会的进步，引领着时代的前行。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 11, "环保行动积极努力保护地球的生物多样性，为我们共同的家园筑起绿色的屏障。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 12, "外交谈判在国际舞台上寻求和平解决冲突，致力于构建一个更加和谐的世界。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 13, "古代哲学的智慧至今仍对现代社会的诸多难题提供启示和解答，影响深远。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 14, "经济学理论围绕市场体系的优劣进行了深入的探讨与辩论，对经济发展有重要指导意义。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 15, "随着科技的不断进步，军事战略也在不断演变，应对新时代的挑战和需求。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 16, "现代物理学理论深入挖掘宇宙的奥秘，试图解开那些探索宇宙时的未知之谜。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 17, "在医学领域，化学化合物的作用至关重要，它们在许多重大医疗突破中扮演了核心角色。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 18, "当代哲学家在探讨人工智能时代的伦理道德问题，对机器与人类的关系进行深刻反思。"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 19, "不同文化背景下的婚礼仪式代表着一生的承诺与责任，象征着两颗心的永恒结合。"));
        ASSERT_TRUE(tantivy_writer_commit(indexDirectory));
        ASSERT_TRUE(tantivy_load_index(indexDirectory));
    }

    void indexSomeEnglishDocs(const string& englishTokenizerAndParameter, bool docStore){
        ASSERT_TRUE(tantivy_create_index_with_tokenizer(indexDirectory, englishTokenizerAndParameter, docStore));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 0, "Ancient empires rise and fall, shaping history's course."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 1, "Artistic expressions reflect diverse cultural heritages."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 2, "Social movements transform societies, forging new paths."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 3, "Economies fluctuate🦠, reflecting the complex interplay of global forces."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 4, "Strategic military campaigns alter the balance of power."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 5, "Quantum leaps redefine understanding of physical laws."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 6, "Chemical reactions unlock mysteries of nature."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 7, "Philosophical debates ponder the essence of existence.🦠"));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 8, "Marriages blend traditions, celebrating love's union."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 9, "Explorers discover uncharted territories, expanding world maps."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 10, "Innovations 🦠 in technology drive societal progress."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 11, "Environmental conservation efforts protect Earth's biodiversity."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 12, "Diplomatic negotiations seek to resolve international conflicts."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 13, "Ancient philosophies provide wisdom for modern dilemmas."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 14, "Economic theories debate the merits of market systems."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 15, "Military strategies evolve with technological advancements."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 16, "Physics theories delve into the universe's mysteries."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 17, "Chemical compounds play crucial roles in medical breakthroughs."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 18, "Philosophers debate ethics in the age of artificial intelligence."));
        ASSERT_TRUE(tantivy_index_doc(indexDirectory, 19, "Wedding ceremonies across cultures symbo🦠lize lifelong commitment."));
        ASSERT_TRUE(tantivy_writer_commit(indexDirectory));
        ASSERT_TRUE(tantivy_load_index(indexDirectory));
    }
};

TEST_F(FunctionalFFITest, tantivyDeleteRowIds) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("en_stem(true)", false);
        Vec<RowIdWithScore> beforeDeleteTerm = tantivy_bm25_search(indexDirectory, "Ancient", 10, false);
        ASSERT_TRUE(beforeDeleteTerm.size()==2);
        ASSERT_TRUE(tantivy_delete_row_ids(indexDirectory, {0, 13, 1000}));
        Vec<RowIdWithScore> afterDeleteTerm = tantivy_bm25_search(indexDirectory, "Ancient", 10, false);
        ASSERT_TRUE(afterDeleteTerm.size()==0);
    });
}

TEST_F(FunctionalFFITest, TantivySearchInRowIdRange) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("en_stem(true)", false);
        ASSERT_TRUE(tantivy_search_in_rowid_range(indexDirectory, "Ancient", 0, 13, false));
        ASSERT_FALSE(tantivy_search_in_rowid_range(indexDirectory, "Ancient", 17, 22, false));
        ASSERT_FALSE(tantivy_search_in_rowid_range(indexDirectory, "Ancient", 29, 33, false));
    });
}

TEST_F(FunctionalFFITest, TantivyCountInRowIdRange) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("en_stem(true)", false);
        ASSERT_TRUE(tantivy_count_in_rowid_range(indexDirectory, "Ancient", 0, 13, false)==2);
        ASSERT_TRUE(tantivy_count_in_rowid_range(indexDirectory, "Ancient", 0, 12, false)==1);
        ASSERT_TRUE(tantivy_count_in_rowid_range(indexDirectory, "Ancient", 5, 12, false)==0);
    });
}

TEST_F(FunctionalFFITest, TantivySearchInRowIdRangeRegexWithRawTokenizer) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("raw", false);
        ASSERT_TRUE(tantivy_search_in_rowid_range(indexDirectory, "%🦠%", 0, 19, true));
    });
}
TEST_F(FunctionalFFITest, TantivySearchInRowIdRangeRegexWithDefaultTokenizer) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("default", false);
        ASSERT_FALSE(tantivy_search_in_rowid_range(indexDirectory, "%🦠%", 0, 19, true));
    });
}

TEST_F(FunctionalFFITest, TantivyCountInRowIdRangeRegexWithRawTokenizer) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("raw", false);
        ASSERT_TRUE(tantivy_count_in_rowid_range(indexDirectory, "%🦠%", 0, 19, true)==4);
    });
}
TEST_F(FunctionalFFITest, TantivyCountInRowIdRangeRegexWithDefaultTokenizer) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("default", false);
        ASSERT_TRUE(tantivy_count_in_rowid_range(indexDirectory, "%🦠%", 0, 19, true)==0);
    });
}

TEST_F(FunctionalFFITest, TantivyBM25Search) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("en_stem(false)", false);
        Vec<RowIdWithScore> result = tantivy_bm25_search(indexDirectory, "the of", 3, false);
        ASSERT_TRUE(result[0].row_id == 7);
        ASSERT_TRUE(result[0].doc == "");
        ASSERT_TRUE(result[1].row_id == 4);
        ASSERT_TRUE(result[1].doc == "");
        ASSERT_TRUE(result[2].row_id == 14);
        ASSERT_TRUE(result[2].doc == "");
    });
}

TEST_F(FunctionalFFITest, TantivyBM25SearchWithDocStore) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("en_stem(false)", true);
        Vec<RowIdWithScore> result = tantivy_bm25_search(indexDirectory, "the of", 3, true);
        ASSERT_TRUE(result[0].row_id == 7);
        ASSERT_TRUE(result[0].doc != "");
        ASSERT_TRUE(result[1].row_id == 4);
        ASSERT_TRUE(result[1].doc != "");
        ASSERT_TRUE(result[2].row_id == 14);
        ASSERT_TRUE(result[2].doc != "");
    });
}

TEST_F(FunctionalFFITest, TantivyBM25SearchDocStoreWithStopWords) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("en_stem(true)", true);
        ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectory, "the of", 3, true));
        Vec<RowIdWithScore> result = tantivy_bm25_search(indexDirectory, "the of abc", 3, true);
        ASSERT_TRUE(result.size() == 0);
    });
}

TEST_F(FunctionalFFITest, TantivyBM25SearchWithStopWords) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("en_stem(true)", false);
        ASSERT_ANY_THROW(tantivy_bm25_search(indexDirectory, "the of", 3, false));
        Vec<RowIdWithScore> result = tantivy_bm25_search(indexDirectory, "the of abc", 3, false);
        ASSERT_TRUE(result.size() == 0);
    });
}

TEST_F(FunctionalFFITest, TantivyBM25SearchWithFilter) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("en_stem(false)", false);
        vector<uint8_t> aliveRowIds;
        aliveRowIds.push_back(128); // 10000000 -> 7
        aliveRowIds.push_back(64); // 01000000 -> 14
        Vec<RowIdWithScore> result = tantivy_bm25_search_with_filter(indexDirectory, "the of", aliveRowIds, 10, false);
        ASSERT_TRUE(result.size() == 2);
        ASSERT_TRUE(result[0].row_id == 7);
        ASSERT_TRUE(result[0].doc == "");
        ASSERT_TRUE(result[1].row_id == 14);
        ASSERT_TRUE(result[1].doc == "");
    });
}

TEST_F(FunctionalFFITest, TantivySearchBitmapResults) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("en_stem(false)", true);
        Vec<uint8_t> result = tantivy_search_bitmap_results(indexDirectory, "Ancient", false);
        // 00000001(1) -> 0
        ASSERT_TRUE(result[0]==1);
        // 00100000(32) -> 13
        ASSERT_TRUE(result[1]==32);
    });
}

TEST_F(FunctionalFFITest, TantivySearchBitmapResultsRegex) {
    ASSERT_NO_THROW({
        indexSomeEnglishDocs("default", true);
        Vec<uint8_t> result = tantivy_search_bitmap_results(indexDirectory, "%ste%", true);
        // 11000000(192) -> 6, 7
        ASSERT_TRUE(result[0]==192);
        // 01000000(64) -> 14
        ASSERT_TRUE(result[1]==64);
        // 00000001(1) -> 16
        ASSERT_TRUE(result[2]==1);
    });
}
