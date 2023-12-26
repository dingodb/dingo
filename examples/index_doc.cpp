#include <iostream>
#include <vector>
#include <tantivy_search.h>
#include <filesystem>
#include <utils.h>

namespace fs = std::filesystem;

using namespace Utils;
using namespace std;

void test_default_create()
{
    fs::remove_all("./temp");
    tantivy_logger_initialize("/home/mochix/workspace_github/tantivy-search/log/", "info", false, tantivy_log_callback, true, true);
    TantivySearchIndexW * indexW = tantivy_create_index("./temp");
    tantivy_index_doc(indexW, 0, "Ancient empires rise and fall, shaping history's course.");
    tantivy_index_doc(indexW, 1, "Artistic expressions reflect diverse cultural heritages.");
    tantivy_index_doc(indexW, 2, "Social movements transform societies, forging new paths.");
    tantivy_index_doc(indexW, 3, "Economies fluctuate, reflecting the complex interplay of global forces.");
    tantivy_index_doc(indexW, 4, "Strategic military campaigns alter the balance of power.");
    tantivy_index_doc(indexW, 5, "Quantum leaps redefine understanding of physical laws.");
    tantivy_index_doc(indexW, 6, "Chemical reactions unlock mysteries of nature.");
    tantivy_index_doc(indexW, 7, "Philosophical debates ponder the essence of existence.");
    tantivy_index_doc(indexW, 8, "Marriages blend traditions, celebrating love's union.");
    tantivy_index_doc(indexW, 9, "Explorers discover uncharted territories, expanding world maps.");
    tantivy_index_doc(indexW, 10, "Innovations in technology drive societal progress.");
    tantivy_index_doc(indexW, 11, "Environmental conservation efforts protect Earth's biodiversity.");
    tantivy_index_doc(indexW, 12, "Diplomatic negotiations seek to resolve international conflicts.");
    tantivy_index_doc(indexW, 13, "Ancient philosophies provide wisdom for modern dilemmas.");
    tantivy_index_doc(indexW, 14, "Economic theories debate the merits of market systems.");
    tantivy_index_doc(indexW, 15, "Military strategies evolve with technological advancements.");
    tantivy_index_doc(indexW, 16, "Physics theories delve into the universe's mysteries.");
    tantivy_index_doc(indexW, 17, "Chemical compounds play crucial roles in medical breakthroughs.");
    tantivy_index_doc(indexW, 18, "Philosophers debate ethics in the age of artificial intelligence.");
    tantivy_index_doc(indexW, 19, "Wedding ceremonies across cultures symbolize lifelong commitment.");

    tantivy_writer_commit(indexW);
    tantivy_writer_free(indexW);
    // search
    TantivySearchIndexR * indexR = tantivy_load_index("./temp");
    // int searched = tantivy_count_in_rowid_range(indexR, "The cat sleeps as the sun sets.", 0, 14);
    int searched = tantivy_count_in_rowid_range(indexR, "redefi*", 0, 19, false);
    cout << "searched:" << searched << endl;

    tantivy_reader_free(indexR);
}

void test_regx_create()
{
    fs::remove_all("./temp");
    tantivy_logger_initialize("/home/mochix/workspace_github/tantivy-search/log/", "info", true, tantivy_log_callback, true, true);
    TantivySearchIndexW * indexW = tantivy_create_index("./temp");
    tantivy_index_doc(indexW, 0, "Ancient empires rise and fall, shaping history's course.");
    tantivy_index_doc(indexW, 1, "Artistic expressions reflect diverse cultural heritages.");
    tantivy_index_doc(indexW, 2, "Social movements transform societies, forging new paths.");
    tantivy_index_doc(indexW, 3, "Economies fluctuate, reflecting the complex interplay of global forces.");
    tantivy_index_doc(indexW, 4, "Strategic military campaigns alter the balance of power.");
    tantivy_index_doc(indexW, 5, "Quantum leaps redefine understanding of physical laws.");
    tantivy_index_doc(indexW, 6, "Chemical reactions unlock mysteries of nature.");
    tantivy_index_doc(indexW, 7, "Philosophical debates ponder the essence of existence.");
    tantivy_index_doc(indexW, 8, "Marriages blend traditions, celebrating love's union.");
    tantivy_index_doc(indexW, 9, "Explorers discover uncharted territories, expanding world maps.");
    tantivy_index_doc(indexW, 10, "Innovations in technology drive societal progress.");
    tantivy_index_doc(indexW, 11, "Environmental conservation efforts protect Earth's biodiversity.");
    tantivy_index_doc(indexW, 12, "Diplomatic negotiations seek to resolve international conflicts.");
    tantivy_index_doc(indexW, 13, "Ancient philosophies provide wisdom for modern dilemmas.");
    tantivy_index_doc(indexW, 14, "Economic theories debate the merits of market systems.");
    tantivy_index_doc(indexW, 15, "Military strategies evolve with technological advancements.");
    tantivy_index_doc(indexW, 16, "Physics theories delve into the universe's mysteries.");
    tantivy_index_doc(indexW, 17, "Chemical compounds play crucial roles in medical breakthroughs.");
    tantivy_index_doc(indexW, 18, "Philosophers debate ethics in the age of artificial intelligence.");
    tantivy_index_doc(indexW, 19, "Wedding ceremonies acro%ss cultures symbolize lifelong commitment.");
    tantivy_index_doc(indexW, 20, "cultures");
    // tantivy_index_doc(indexW, 20, "commitment");

    tantivy_writer_commit(indexW);
    tantivy_writer_free(indexW);
    // search
    TantivySearchIndexR * indexR = tantivy_load_index("./temp");
    int searched = tantivy_count_in_rowid_range(indexR, "%\\%%", 19, 19, true);
    // bool searched = tantivy_regex_count_in_rowid_range(indexR, ".*me.*", 20, 20);
    cout << "searched:" << searched << endl;

    tantivy_reader_free(indexR);
}

void test_tokenizer_create()
{
    fs::remove_all("./temp");
    // fs::remove_all("./log");
    tantivy_logger_initialize("/home/mochix/workspace_github/tantivy-search/log/", "info", false, tantivy_log_callback, true, true);
    TantivySearchIndexW * indexW = tantivy_create_index_with_language("./temp", "chinese");
    tantivy_index_doc(indexW, 0, "古代帝国的兴衰更迭，不仅塑造了历史的进程，也铭刻了时代的变迁与文明的发展。");
    tantivy_index_doc(indexW, 1, "艺术的多样表达方式反映了不同文化的丰富遗产，展现了人类创造力的无限可能。");
    tantivy_index_doc(indexW, 2, "社会运动如同时代的浪潮，改变着社会的面貌，为历史开辟新的道路和方向。");
    tantivy_index_doc(indexW, 3, "全球经济的波动复杂多变，如同镜子反映出世界各国之间错综复杂的力量关系。");
    tantivy_index_doc(indexW, 4, "战略性的军事行动改变了世界的权力格局，也重新定义了国际政治的均势。");
    tantivy_index_doc(indexW, 5, "量子物理的飞跃性进展，彻底改写了我们对物理世界规律的理解和认知。");
    tantivy_index_doc(indexW, 6, "化学反应不仅揭开了大自然奥秘的一角，也为科学的探索提供了新的窗口。");
    tantivy_index_doc(indexW, 7, "哲学家的辩论深入探讨了生命存在的本质，引发人们对生存意义的深刻思考。");
    tantivy_index_doc(indexW, 8, "婚姻的融合不仅是情感的结合，更是不同传统和文化的交汇，彰显了爱的力量。");
    tantivy_index_doc(indexW, 9, "勇敢的探险家发现了未知的领域，为人类的世界观增添了新的地理篇章。");
    tantivy_index_doc(indexW, 10, "科技创新的步伐从未停歇，它推动着社会的进步，引领着时代的前行。");
    tantivy_index_doc(indexW, 11, "环保行动积极努力保护地球的生物多样性，为我们共同的家园筑起绿色的屏障。");
    tantivy_index_doc(indexW, 12, "外交谈判在国际舞台上寻求和平解决冲突，致力于构建一个更加和谐的世界。");
    tantivy_index_doc(indexW, 13, "古代哲学的智慧至今仍对现代社会的诸多难题提供启示和解答，影响深远。");
    tantivy_index_doc(indexW, 14, "经济学理论围绕市场体系的优劣进行了深入的探讨与辩论，对经济发展有重要指导意义。");
    tantivy_index_doc(indexW, 15, "随着科技的不断进步，军事战略也在不断演变，应对新时代的挑战和需求。");
    tantivy_index_doc(indexW, 16, "现代物理学理论深入挖掘宇宙的奥秘，试图解开那些探索宇宙时的未知之谜。");
    tantivy_index_doc(indexW, 17, "在医学领域，化学化合物的作用至关重要，它们在许多重大医疗突破中扮演了核心角色。");
    tantivy_index_doc(indexW, 18, "当代哲学家在探讨人工智能时代的伦理道德问题，对机器与人类的关系进行深刻反思。");
    tantivy_index_doc(indexW, 19, "不同文化背景下的婚礼仪式代表着一生的承诺与责任，象征着两颗心的永恒结合。");
    tantivy_logger_initialize("/home/mochix/workspace_github/tantivy-search/log/", "info", true, tantivy_log_callback, true, true);
    tantivy_writer_commit(indexW);
    tantivy_writer_free(indexW);
    // search
    TantivySearchIndexR * indexR = tantivy_load_index("./temp");
    int searched_for_chinese = tantivy_count_in_rowid_range(indexR, "影响深远", 0, 19, false);
    cout << "searched_for_chinese:" << searched_for_chinese << endl;

    tantivy_reader_free(indexR);
}



int main(){
    // test_default_create();
    test_regx_create();
    // test_tokenizer_create();
    return 0;
}


