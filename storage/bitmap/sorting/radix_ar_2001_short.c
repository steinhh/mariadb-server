#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

// complicated expression better fits as macro (or inline in C++)
#define ByteOf(x) (*(uchar *) (x))

typedef unsigned long ulong;
// typedef unsigned long long size_t;
typedef unsigned char uchar;

// replaced byte with bitsOffset to avoid *8 operation in loop
static void radix (short byteOffset, size_t N, ulong *source, ulong *dest)
{
  // suppressed the need for index as it is reported in counts
  const Nbuckets = 256*256;

  size_t counts[Nbuckets];
  
  // added temp variables to simplify writing, understanding and compiler optimization job
  // most of them will be allocated as registers
  ulong *source_p, *count_p, run_sum, c, i;
  uchar *bp;
  
  // faster than MemSet
  count_p = counts;
  for (i = Nbuckets; i > 0; --i, ++count_p)
    *count_p = 0;
  
  // count occurences of every byte value
  bp = ((uchar *) source) + byteOffset;
  for (i = N; i > 0; --i, bp += 4) {
    count_p = counts + *bp + 256 * *(bp+1);
    ++(*count_p);
  }
  
  // transform count into index by summing elements and storing into same array
  run_sum = 0;
  count_p = counts;
  for (i = Nbuckets; i > 0; --i, ++count_p) {
    c = *count_p;
    *count_p = run_sum;
    run_sum += c;
  }
  
  // fill dest with the right values in the right place
  bp = ((uchar *) source) + byteOffset;
  source_p = source;
  for (i = N; i > 0; --i, bp += 4, ++source_p) {
    count_p = counts + *bp + 256 * *(bp+1);
    dest[*count_p] = *source_p;
    ++(*count_p);
  }
}

static void radix_sort (ulong *source, size_t N)
{
  // allocate heap memory to avoid the need of additional parameter
  ulong *temp = (ulong *) malloc (N * sizeof (ulong));
  assert (temp != NULL);

  radix (0, N, temp, source);
  radix (2, N, temp, source);

  free (temp);
}

static void make_random (ulong *data, size_t N)
{
  for ( ; N > 0; --N, ++data)
    *data = rand () | (rand () << 16);
}

static void check_order (ulong *data, size_t N)
{
  // only signal errors if any (should not be)
  --N;
  for ( ; N > 0; --N, ++data)
    assert (data[0] <= data[1]);
}

// test for big number of elements
static void test_radix (size_t N)
{
  ulong *data = (ulong *) malloc (N * sizeof (ulong));
  assert (data != NULL);

  make_random (data, N);
  radix_sort (data, N);
  check_order (data, N);

  free (data);
}

int main (int argc, const char ** argv)
{
  if ( argc != 2 ) {
    printf("Must give size of array!\n");
    exit(1);
  }

  unsigned long long size = atoll(argv[1]);

  printf("Size: %llu\n",size);

  test_radix (size);

}
