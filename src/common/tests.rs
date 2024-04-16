use std::{cmp::min, sync::Arc};

use tantivy::{
    collector::Count,
    merge_policy::LogMergePolicy,
    query::QueryParser,
    schema::{Schema, FAST, INDEXED, TEXT},
    Index, IndexReader, IndexWriter, TantivyDocument,
};

use crate::{index::bridge::index_writer_bridge::IndexWriterBridge, FFI_INDEX_WRITER_CACHE};

#[allow(dead_code)]
pub fn get_mocked_docs() -> (Vec<String>, Vec<String>, Vec<String>) {
    let col1_docs: Vec<String> = vec![
        "Ancient empires rise and fall, shaping history's course.".to_string(),
        "Artistic expressions reflect diverse cultural heritages.".to_string(),
        "Social movements transform societies, forging new paths.".to_string(),
        "Strategic military campaigns alter the balance of power.".to_string(),
        "Ancient philosophies provide wisdom for modern dilemmas.".to_string(),
    ];
    let col2_docs: Vec<String> = vec![
        "Brave explorers venture into uncharted territories, expanding horizons.".to_string(),
        "Brilliant minds unravel nature's judgment through scientific inquiry.".to_string(),
        "Economic systems evolve, influencing global trade and prosperity.".to_string(),
        "Environmental challenges demand innovative solutions for sustainability.".to_string(),
        "Ethical dilemmas test the boundaries of moral reasoning and Judgment.".to_string(),
    ];

    let col3_docs: Vec<String> = vec![
        "Groundbreaking inventions revolutionize industries and daily life.".to_string(),
        "Iconic leaders inspire generations with their vision and charisma.".to_string(),
        "Literary masterpieces capture the essence of the human experience.".to_string(),
        "Majestic natural wonders showcase the breathtaking beauty of Earth.".to_string(),
        "Philosophical debates shape our understanding of reality and existence.".to_string(),
    ];
    return (col1_docs, col2_docs, col3_docs);
}

#[allow(dead_code)]
fn create_3column_schema() -> Schema {
    // Construct the schema for the index.
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("row_id", FAST | INDEXED);
    schema_builder.add_text_field("col1", TEXT);
    schema_builder.add_text_field("col2", TEXT);
    schema_builder.add_text_field("col3", TEXT);
    let schema = schema_builder.build();
    return schema;
}

#[allow(dead_code)]
fn create_3column_index(index_directory: &str) -> (Index, Schema) {
    let schema = create_3column_schema();
    // Create the index in the specified directory.
    let index = Index::create_in_dir(index_directory, schema.clone()).unwrap();
    (index, schema)
}

#[allow(dead_code)]
pub fn index_documents(
    writer: &mut IndexWriter,
    schema: &Schema,
    col1_docs: &[String],
    col2_docs: &[String],
    col3_docs: &[String],
) {
    // Get fields from `schema`.
    let row_id_field = schema.get_field("row_id").unwrap();
    let col1_field = schema.get_field("col1").unwrap();
    let col2_field = schema.get_field("col2").unwrap();
    let col3_field = schema.get_field("col3").unwrap();

    // Index some documents.
    for row_id in 0..min(min(col1_docs.len(), col2_docs.len()), col3_docs.len()) {
        let mut doc = TantivyDocument::default();
        doc.add_u64(row_id_field, row_id as u64);
        doc.add_text(col1_field, &col1_docs[row_id]);
        doc.add_text(col2_field, &col2_docs[row_id]);
        doc.add_text(col3_field, &col3_docs[row_id]);
        let result = writer.add_document(doc);
        assert!(result.is_ok());
    }
}

#[allow(dead_code)]
pub fn index_3column_docs_with_threads_merge(index_directory: &str) -> (IndexReader, Schema) {
    let (index, schema) = create_3column_index(index_directory);

    // Create the writer with a specified buffer size (e.g., 64 MB).
    let mut writer = index.writer_with_num_threads(2, 1024 * 1024 * 64).unwrap();
    // Configure default merge policy.
    writer.set_merge_policy(Box::new(LogMergePolicy::default()));

    // Index some documents.
    let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();
    index_documents(&mut writer, &schema, &col1_docs, &col2_docs, &col3_docs);

    assert!(writer.commit().is_ok());
    assert!(writer.wait_merging_threads().is_ok());

    (index.reader().unwrap(), schema)
}

#[allow(dead_code)]
pub fn index_3column_docs_without_threads_merge(
    index_directory: &str,
) -> (IndexWriter, IndexReader, Schema) {
    let (index, schema) = create_3column_index(index_directory);

    // Create the writer with a specified buffer size (e.g., 64 MB).
    let mut writer = index.writer_with_num_threads(2, 1024 * 1024 * 64).unwrap();
    // Configure default merge policy.
    writer.set_merge_policy(Box::new(LogMergePolicy::default()));

    // Index some documents.
    let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();
    index_documents(&mut writer, &schema, &col1_docs, &col2_docs, &col3_docs);

    assert!(writer.commit().is_ok());

    (writer, index.reader().unwrap(), schema)
}

#[allow(dead_code)]
pub fn index_3column_docs_with_index_writer_bridge(
    index_directory: &str,
    waiting_merging_threads_finished: bool,
) -> Arc<IndexWriterBridge> {
    // Get index writer from CACHE
    let index_writer_bridge = FFI_INDEX_WRITER_CACHE
        .get_index_writer_bridge(index_directory.to_string())
        .unwrap();
    let (col1_docs, col2_docs, col3_docs) = get_mocked_docs();
    let schema = create_3column_schema();

    // Get fields from `schema`.
    let row_id_field = schema.get_field("row_id").unwrap();
    let col1_field = schema.get_field("col1").unwrap();
    let col2_field = schema.get_field("col2").unwrap();
    let col3_field = schema.get_field("col3").unwrap();

    for row_id in 0..min(min(col1_docs.len(), col2_docs.len()), col3_docs.len()) {
        let mut doc = TantivyDocument::default();
        doc.add_u64(row_id_field, row_id as u64);
        doc.add_text(col1_field, &col1_docs[row_id]);
        doc.add_text(col2_field, &col2_docs[row_id]);
        doc.add_text(col3_field, &col3_docs[row_id]);
        let result = index_writer_bridge.add_document(doc);
        assert!(result.is_ok());
    }
    assert!(index_writer_bridge.commit().is_ok());
    if waiting_merging_threads_finished {
        assert!(index_writer_bridge.wait_merging_threads().is_ok());
    }

    index_writer_bridge
}

#[allow(dead_code)]
pub fn search_with_index_writer_bridge(index_writer_bridge: Arc<IndexWriterBridge>) {
    // Get fields from `schema`.
    let schema = index_writer_bridge.index.schema();
    let col1_field = schema.get_field("col1").unwrap();
    let col2_field = schema.get_field("col2").unwrap();
    let col3_field = schema.get_field("col3").unwrap();
    let parser_col1 = QueryParser::for_index(&index_writer_bridge.index, vec![col1_field]);
    let parser_col2 = QueryParser::for_index(&index_writer_bridge.index, vec![col2_field]);
    let parser_col3 = QueryParser::for_index(&index_writer_bridge.index, vec![col3_field]);
    let parser_all = QueryParser::for_index(
        &index_writer_bridge.index,
        vec![col1_field, col2_field, col3_field],
    );

    let text_query_in_col1 = parser_col1.parse_query("of").unwrap();
    let text_query_in_col2 = parser_col2.parse_query("of").unwrap();
    let text_query_in_col3 = parser_col3.parse_query("of").unwrap();
    let text_query_in_all = parser_all.parse_query("of").unwrap();

    // Test whether index can be use.
    let searcher = index_writer_bridge.index.reader().unwrap().searcher();
    let count_1 = searcher.search(&text_query_in_col1, &Count).unwrap();
    let count_2 = searcher.search(&text_query_in_col2, &Count).unwrap();
    let count_3 = searcher.search(&text_query_in_col3, &Count).unwrap();
    let count_a = searcher.search(&text_query_in_all, &Count).unwrap();

    assert_eq!(count_1, 1);
    assert_eq!(count_2, 1);
    assert_eq!(count_3, 3);
    assert_eq!(count_a, 3);
}

// Mock data for part0
#[allow(dead_code)]
pub fn get_mocked_docs_for_part0() -> (Vec<String>, Vec<String>, Vec<String>) {
    let col1_docs: Vec<String> = vec![
        "Ancient empires rise and fall, shaping history's course.".to_string(),
        "Artistic expressions reflect diverse cultural heritages.".to_string(),
        "Social movements transform societies, forging new paths.".to_string(),
        "Strategic military campaigns alter the balance of power.".to_string(),
        "Ancient philosophies provide wisdom for modern dilemmas.".to_string(),
        "Revolutionary leaders challenge the status quo, inspiring change.".to_string(),
        "Architectural wonders stand as testaments to human creativity.".to_string(),
        "Trade routes expand horizons, connecting distant cultures.".to_string(),
        "Great thinkers challenge societal norms, advancing human thought.".to_string(),
        "Historic discoveries uncover lost civilizations and their secrets.".to_string(),
    ];
    let col2_docs: Vec<String> = vec![
        "Brave explorers venture into uncharted territories, expanding horizons.".to_string(),
        "Brilliant minds unravel nature's judgment through scientific inquiry.".to_string(),
        "Economic systems evolve, influencing global trade and prosperity.".to_string(),
        "Environmental challenges demand innovative solutions for sustainability.".to_string(),
        "Ethical dilemmas test the boundaries of moral reasoning and judgment.".to_string(),
        "Technological innovations disrupt industries, creating new markets.".to_string(),
        "Educational reforms empower future generations with knowledge.".to_string(),
        "Civic movements advocate for justice and equality.".to_string(),
        "Art and music fuse to express the unspoken language of cultures.".to_string(),
        "Medicine advances, pushing the boundaries of human health and longevity.".to_string(),
    ];
    let col3_docs: Vec<String> = vec![
        "Groundbreaking inventions revolutionize industries and daily life.".to_string(),
        "Iconic leaders inspire generations with their vision and charisma.".to_string(),
        "Literary masterpieces capture the essence of the human experience.".to_string(),
        "Majestic natural wonders showcase the breathtaking beauty of Earth.".to_string(),
        "Philosophical debates shape our understanding of reality and existence.".to_string(),
        "Scientific breakthroughs offer solutions to global challenges.".to_string(),
        "Humanitarian efforts alleviate suffering and provide hope.".to_string(),
        "Sustainable practices protect ecosystems for future generations.".to_string(),
        "Digital transformation reshapes the way societies function.".to_string(),
        "Athletic achievements inspire excellence and unity in sports.".to_string(),
    ];
    return (col1_docs, col2_docs, col3_docs);
}

// Mock data for part1 (Modified to include more rows)
#[allow(dead_code)]
pub fn get_mocked_docs_for_part1() -> (Vec<String>, Vec<String>, Vec<String>) {
    let col1_docs: Vec<String> = vec![
        "Technological advancements redefine the future of work and leisure.".to_string(),
        "Historic treaties shape the geopolitical landscape of nations.".to_string(),
        "Culinary traditions blend to create unique global cuisines.".to_string(),
        "Dynamic educational methods reshape learning paradigms.".to_string(),
        "Vibrant festivals celebrate the rich tapestry of human cultures.".to_string(),
        "Innovative art forms emerge, blending tradition with modernity.".to_string(),
        "Migration patterns influence cultural exchanges and societal integration.".to_string(),
        "Social media revolutionizes communication, fostering global connections.".to_string(),
        "Climate change advocacy prompts action and policy change.".to_string(),
        "Entrepreneurial ventures spur economic growth and innovation.".to_string(),
    ];
    let col2_docs: Vec<String> = vec![
        "Innovators pioneer sustainable energy solutions to combat climate change.".to_string(),
        "Researchers decode genetic mysteries, unlocking new medical treatments.".to_string(),
        "Financial markets adapt to emerging technologies and changing economies.".to_string(),
        "Urban planners design smart cities for increased livability and efficiency.".to_string(),
        "Human rights movements advocate for equality and justice worldwide.".to_string(),
        "Autonomous vehicles transform the transportation industry.".to_string(),
        "Cybersecurity measures intensify in response to growing threats.".to_string(),
        "Space exploration reaches new frontiers, aiming for Mars colonization.".to_string(),
        "Renewable resources gain prominence, reducing reliance on fossil fuels.".to_string(),
        "Cultural heritage sites receive modern tech for preservation and education.".to_string(),
    ];
    let col3_docs: Vec<String> = vec![
        "Pioneering space missions explore the uncharted realms of the cosmos.".to_string(),
        "Renowned artists disrupt traditional mediums with digital art.".to_string(),
        "Global collaborations foster peace and understanding among nations.".to_string(),
        "Revolutionary sports techniques enhance athlete performance and safety.".to_string(),
        "Scientific debates highlight the ethical considerations of AI advancements.".to_string(),
        "Virtual reality revolutionizes training and education sectors.".to_string(),
        "Oceanic research vessels uncover mysteries of the deep sea.".to_string(),
        "Archaeological findings rewrite history with new discoveries.".to_string(),
        "Telehealth becomes integral to modern healthcare systems.".to_string(),
        "Advancements in robotics automate tasks, improving efficiency and safety.".to_string(),
    ];
    return (col1_docs, col2_docs, col3_docs);
}

// Mock data for part2 (Modified to include more rows)
#[allow(dead_code)]
pub fn get_mocked_docs_for_part2() -> (Vec<String>, Vec<String>, Vec<String>) {
    let col1_docs: Vec<String> = vec![
        "Revival of ancient crafts revives local economies and traditions.".to_string(),
        "Documentaries spotlight critical environmental and social issues.".to_string(),
        "Political shifts lead to new alliances and policy reforms.".to_string(),
        "Public health initiatives combat global pandemics and improve wellness.".to_string(),
        "Innovative startups disrupt old industries with new technologies.".to_string(),
        "Elderly care innovations enhance quality of life and independence.".to_string(),
        "Youth engagement in politics reshapes future leadership landscapes.".to_string(),
        "Sustainable tourism practices promote environmental stewardship.".to_string(),
        "Multilingual education fosters global communication and understanding.".to_string(),
        "Blockchain technology secures transactions and empowers digital trust.".to_string(),
    ];
    let col2_docs: Vec<String> = vec![
        "Expeditions reveal secrets of the deep sea and expand marine biology.".to_string(),
        "Quantum computing breakthroughs accelerate problem-solving capabilities.".to_string(),
        "Sustainable agriculture practices enhance food security and biodiversity.".to_string(),
        "AI ethics frameworks guide responsible development of smart technologies.".to_string(),
        "Preservation efforts restore endangered habitats and species.".to_string(),
        "Nanotechnology revolutionizes materials science and engineering.".to_string(),
        "Genetic editing techniques promise cures for hereditary diseases.".to_string(),
        "Smart grid technologies optimize energy distribution and consumption.".to_string(),
        "Wearable health devices monitor vital signs for early detection of diseases.".to_string(),
        "Cognitive science advances enhance understanding of brain functions.".to_string(),
    ];
    let col3_docs: Vec<String> = vec![
        "Interstellar missions search for extraterrestrial life and habitable planets.".to_string(),
        "Museum exhibitions educate and inspire with historical and artistic insights.".to_string(),
        "Urban agriculture initiatives bring farms to city rooftops and balconies.".to_string(),
        "Youth empowerment programs develop skills and confidence in young people.".to_string(),
        "Deep learning algorithms transform data analytics and business intelligence.".to_string(),
        "Eco-friendly packaging solutions reduce waste and pollution.".to_string(),
        "Crisis response strategies improve disaster preparedness and recovery.".to_string(),
        "Performance art merges theater, music, and visual arts to challenge conventions."
            .to_string(),
        "Digital currencies facilitate faster and more secure financial transactions.".to_string(),
        "Telecommuting tools become essential for remote work and global teams.".to_string(),
    ];
    return (col1_docs, col2_docs, col3_docs);
}

// Mock data for part3 (Modified to include more rows)
#[allow(dead_code)]
pub fn get_mocked_docs_for_part3() -> (Vec<String>, Vec<String>, Vec<String>) {
    let col1_docs: Vec<String> = vec![
        "Classical music orchestras innovate with modern compositions.".to_string(),
        "Digital literacy programs bridge the gap between generations.".to_string(),
        "Healthcare equity becomes a primary focus in policy development.".to_string(),
        "Art conservation techniques evolve with new science and technology.".to_string(),
        "Public transportation upgrades reduce congestion and pollution.".to_string(),
        "Heritage languages are revitalized through educational programs.".to_string(),
        "Urban renewal projects transform declining areas into vibrant communities.".to_string(),
        "Data privacy laws strengthen protection for consumers.".to_string(),
        "Microfinance institutions support small businesses in developing countries.".to_string(),
        "Disaster-resistant infrastructure mitigates the effects of extreme weather.".to_string(),
    ];
    let col2_docs: Vec<String> = vec![
        "Agricultural drones improve crop monitoring and management.".to_string(),
        "Biodiversity research drives conservation efforts worldwide.".to_string(),
        "E-learning platforms expand access to education across borders.".to_string(),
        "Mass transit systems innovate with green technology.".to_string(),
        "Nutrition science advances understanding of diet and health.".to_string(),
        "Renewable energy projects proliferate, driven by policy and technology.".to_string(),
        "Social entrepreneurship tackles societal issues with innovative business models."
            .to_string(),
        "Virtual museums make art accessible to a global audience.".to_string(),
        "Water purification technologies address global drinking water shortages.".to_string(),
        "Wildlife corridors facilitate animal movement and habitat connectivity.".to_string(),
    ];
    let col3_docs: Vec<String> = vec![
        "Augmented reality applications enhance user experiences in various sectors.".to_string(),
        "Biotechnology firms engineer solutions for environmental and health issues.".to_string(),
        "Community gardens increase local food production and community engagement.".to_string(),
        "Drone technology advances impact surveillance, delivery, and entertainment sectors."
            .to_string(),
        "Eco-friendly buildings set new standards for sustainable construction.".to_string(),
        "Futuristic transportation concepts promise speed and sustainability.".to_string(),
        "Genetic research sheds light on diseases and potential therapies.".to_string(),
        "Holographic displays revolutionize entertainment and advertising.".to_string(),
        "Interactive learning tools transform educational experiences.".to_string(),
        "Job automation trends reshape workforce dynamics and skill demands.".to_string(),
    ];
    return (col1_docs, col2_docs, col3_docs);
}

// Mock data for part4 (Modified to include more rows)
#[allow(dead_code)]
pub fn get_mocked_docs_for_part4() -> (Vec<String>, Vec<String>, Vec<String>) {
    let col1_docs: Vec<String> = vec![
        "Smart home technologies enhance convenience and security.".to_string(),
        "Global warming impacts force shifts in agriculture and urban planning.".to_string(),
        "Revitalized public squares become hubs of community and culture.".to_string(),
        "Telemedicine expands access to healthcare services remotely.".to_string(),
        "Sustainable fashion practices gain traction among consumers and designers.".to_string(),
        "Elder care technology supports aging populations with innovative solutions.".to_string(),
        "Youth sports programs promote health, teamwork, and discipline.".to_string(),
        "Recycling initiatives reduce waste and encourage sustainable consumption.".to_string(),
        "Civic tech startups foster government transparency and citizen engagement.".to_string(),
        "Impact investing directs capital to socially and environmentally beneficial projects."
            .to_string(),
    ];
    let col2_docs: Vec<String> = vec![
        "Artificial reefs enhance marine biodiversity and boost ecotourism.".to_string(),
        "Big data analytics drive decision-making in business and government.".to_string(),
        "Community-driven development projects empower local populations.".to_string(),
        "Drone racing emerges as a new sport with global competitions.".to_string(),
        "Electric vehicle adoption accelerates, reducing reliance on fossil fuels.".to_string(),
        "Food security initiatives address malnutrition and food deserts.".to_string(),
        "Green roofing projects combat urban heat islands and improve air quality.".to_string(),
        "High-speed rail networks reduce travel time and carbon footprints.".to_string(),
        "Inclusive design principles guide the creation of accessible products and environments."
            .to_string(),
        "Junk art movements recycle materials into creative and functional pieces.".to_string(),
    ];
    let col3_docs: Vec<String> = vec![
        "Advanced prosthetics improve mobility and quality of life for amputees.".to_string(),
        "Biophilic design incorporates natural elements into urban spaces.".to_string(),
        "Cultural exchange programs enhance understanding and collaboration.".to_string(),
        "Digital detox retreats gain popularity as a way to unplug and unwind.".to_string(),
        "Energy-efficient appliances become standard in homes and businesses.".to_string(),
        "Flood management technologies protect communities from extreme weather events."
            .to_string(),
        "Genome editing tools hold promise for treating genetic disorders.".to_string(),
        "Heritage conservation efforts preserve historical sites for future generations."
            .to_string(),
        "Immersive theater experiences blur the lines between audience and performers.".to_string(),
        "Job training programs adapt to rapidly changing industry demands.".to_string(),
    ];
    return (col1_docs, col2_docs, col3_docs);
}
