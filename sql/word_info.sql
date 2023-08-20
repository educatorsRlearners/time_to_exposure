// Get words with Lexile Score
MATCH (s:Syllable)-[:syllableOf]->(w:Word)
where w.lexileScore is not null   
return w.word as word,
       w.id as word_id,
       w.grade as word_grade,
       w.ageOfAcquisition as age_of_acq,
       w.partOfSpeech as pos,
       COLLECT(s.syllable) as syllables,
       w.web_freq as web_freq,
       w.news_freq as news_freq,
       w.general_freq as gen_freq,
       w.fiction_freq as fict_freq,
       w.wikipedia_freq as wiki_freq,
       w.web_rank as web_rank,
       w.news_rank as news_rank,
       w.fiction_rank as fiction_rank,
       w.general_rank as general_rank,
       w.wikipedia_rank as wiki_rank,
       w.lexileScore as lexile
order by lexile DESC