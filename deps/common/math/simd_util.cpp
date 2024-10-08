/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <stdint.h>
#include "common/math/simd_util.h"

#if defined(USE_SIMD)

int mm256_extract_epi32_var_indx(const __m256i vec, const unsigned int i)
{
  __m128i idx = _mm_cvtsi32_si128(i);
  __m256i val = _mm256_permutevar8x32_epi32(vec, _mm256_castsi128_si256(idx));
  return _mm_cvtsi128_si32(_mm256_castsi256_si128(val));
}

int mm256_sum_epi32(const int *values, int size)
{
  // your code here
  __m256i sum = _mm256_setzero_si256();
  int result = 0;
  for(int i = 0; i < size % 8 ; i++)result += values[i];
  for (int i = size % 8; i < size; i += 8) {
    __m256i vec = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(values + i));
    sum = _mm256_add_epi32(sum, vec);
  }
  int *sum_ptr = reinterpret_cast<int *>(&sum);
  for (int i = 0; i < 8; i++) {
    result += sum_ptr[i];
  }
  return result;
}

float mm256_sum_ps(const float *values, int size)
{
  // your code here
  __m256 sum = _mm256_setzero_ps();
  float result = 0;
  for(int i = 0; i < size % 8 ; i++)result += values[i];
  for (int i = size % 8; i < size; i += 8) {
    __m256 vec = _mm256_loadu_ps(values + i);
    sum = _mm256_add_ps(sum, vec);
  }
  float *sum_ptr = reinterpret_cast<float *>(&sum);
  for (int i = 0; i < 8; i++) {
    result += sum_ptr[i];
  }
  return result;
}

template <typename V>
void selective_load(V *memory, int offset, V *vec, __m256i &inv)
{
  int *inv_ptr = reinterpret_cast<int *>(&inv);
  for (int i = 0; i < SIMD_WIDTH; i++) {
    if (inv_ptr[i] == -1) {
      vec[i] = memory[offset++];
    }
  }
}
template void selective_load<uint32_t>(uint32_t *memory, int offset, uint32_t *vec, __m256i &inv);
template void selective_load<int>(int *memory, int offset, int *vec, __m256i &inv);
template void selective_load<float>(float *memory, int offset, float *vec, __m256i &inv);

template <typename V>
void my_selective_load(V *memory, int offset, V *vec, int *inv)
{
  int *inv_ptr = inv;
  for (int i = 0; i < SIMD_WIDTH; i++) {
    if (inv_ptr[i] == -1) {
      vec[i] = memory[offset++];
    }
  }
}

template <typename V>
void my_selective_load2(V *memory, int& offset, V *vec, int *inv)
{
  int *inv_ptr = inv;
  for (int i = 0; i < SIMD_WIDTH; i++) {
    if (inv_ptr[i] == -1) {
      vec[i] = memory[offset++];
    }
  }
}

template void my_selective_load<uint32_t>(uint32_t *memory, int offset, uint32_t *vec, int *inv);
template void my_selective_load<int>(int *memory, int offset, int *vec, int *inv);
template void my_selective_load<float>(float *memory, int offset, float *vec, int *inv);

template void my_selective_load2<uint32_t>(uint32_t *memory, int& offset, uint32_t *vec, int *inv);
template void my_selective_load2<int>(int *memory, int& offset, int *vec, int *inv);
template void my_selective_load2<float>(float *memory, int& offset, float *vec, int *inv);
#endif