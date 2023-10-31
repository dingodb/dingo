// SPDX-License-Identifier: Apache-2.0

#ifndef TANTIVYSEARCH_H
#define TANTIVYSEARCH_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

struct TantivySearchIndexRW;

struct TantivySearchIterWrapper;

extern "C" {

TantivySearchIndexRW *tantivysearch_open_or_create_index(const char *dir_ptr);

TantivySearchIterWrapper *tantivysearch_search(TantivySearchIndexRW *irw,
                                               const char *query_ptr,
                                               uint64_t limit);

TantivySearchIterWrapper *tantivysearch_ranked_search(TantivySearchIndexRW *irw,
                                                      const char *query_ptr,
                                                      uint64_t limit);

unsigned char tantivysearch_index(TantivySearchIndexRW *irw,
                                  const uint64_t *primary_ids,
                                  const uint64_t *secondary_ids,
                                  const char *chars,
                                  const uint64_t *offsets,
                                  size_t size);

unsigned char tantivysearch_writer_commit(TantivySearchIndexRW *irw);

unsigned char tantivysearch_index_truncate(TantivySearchIndexRW *irw);

unsigned char tantivysearch_iter_next(TantivySearchIterWrapper *iter_ptr,
                                      uint64_t *primary_id_ptr,
                                      uint64_t *secondary_id_ptr);

size_t tantivysearch_iter_batch(TantivySearchIterWrapper *iter_ptr,
                                uint64_t count,
                                uint64_t *primary_ids_ptr,
                                uint64_t *secondary_ids_ptr);

size_t tantivysearch_iter_count(TantivySearchIterWrapper *iter_ptr);

void tantivysearch_iter_free(TantivySearchIterWrapper *iter_ptr);

void tantivysearch_index_free(TantivySearchIndexRW *irw);

void tantivysearch_index_delete(TantivySearchIndexRW *irw);

} // extern "C"

#endif // TANTIVYSEARCH_H