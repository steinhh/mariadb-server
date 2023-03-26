#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <sys/timeb.h>

// Using two bytes at a time means going from
// 111 sec to 83 sec for N = 3 gig. That's 25%!
//
// Using a single run-through to calculate counts gives 71 sec, that's
// another 14%, i.e. this one is 64% faster than the original! :-)

typedef unsigned long ulong;
typedef unsigned char uchar;

// suppressed the need for index as it is reported in counts
#define NBUCKETS 256*256
size_t counts[2][NBUCKETS];
  

// Get counts/sums for all (both) passes in one run-through of the
// data:
//
static void get_counts(size_t N, ulong *source)
{
  int i,j;

  uchar *bp = (uchar*) source;
  size_t number;
  
  // Zero out counts:
  //
  memset( (void*) counts, 0, 2 * NBUCKETS *sizeof(size_t) );
  
  for (i = N; i > 0; --i, bp += sizeof(*source)) {
    number = *(bp+0) + 256 * *(bp+1);
    counts[0][number]++;
    number = *(bp+2) + 256 * *(bp+3);
    counts[1][number]++;
  }

  // Create running sums
  //
  for (j=1; j >=0 ; j--) {
    size_t run_sum = 0;
    for (i=NBUCKETS; i > 0; i--) {
      int count = counts[0][i];
      counts[j][i] = run_sum;
      run_sum += count;
    }
  }
}

// replaced byte with bitsOffset to avoid *8 operation in loop
static void radix (short byteOffset, size_t N, size_t counts[], ulong *source, ulong *dest)
{
  // added temp variables to simplify writing, understanding and compiler optimization job
  // most of them will be allocated as registers
  ulong *source_p, *count_p, run_sum, count, i;
  uchar *bp;
  
  // transform count into index by summing elements and storing into same array
  run_sum = 0;
  count_p = counts;
  for (i = NBUCKETS; i > 0; --i, ++count_p) {
    count = *count_p;
    *count_p = run_sum;
    run_sum += count;
  }
  
  // fill dest with the right values in the right place
  bp = ((uchar *) source) + byteOffset;
  source_p = source;
  for (i = N; i > 0; --i, bp += sizeof(*source), ++source_p) {
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

  get_counts(N, source);
  radix (0, N, counts[0], temp, source);
  radix (2, N, counts[1], temp, source);

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

int compar(const void* first, const void* second)
{
  ulong firstv  =  *((ulong*)first);
  ulong secondv = *((ulong*)second);
  if ( firstv < secondv ) return -1;
  if ( firstv > secondv ) return 1;
  return 0;
}

// test for big number of elements
static void test_radix (size_t N)
{
  ulong *data = (ulong *) malloc (N * sizeof (ulong));
  assert (data != NULL);

  make_random (data, N);

  struct timeb start,end;

  printf("\nStarting sort\n");
  ftime(&start);
  //radix_sort (data, N);
  qsort(data, N, sizeof(*data), compar);

  ftime(&end);
  printf("Ended sort\n");

  double elapse = end.time-start.time + 0.001 *(end.millitm-start.millitm);
  printf("\nTime: %5.2g  (n=%10lu)\n\n",elapse,N);  
  check_order (data, N);

  free (data);
}


static void check_sizes()
{

  typedef long long longlong;

#define ss(p,type1,type2) \
  struct p ## type1 ## _ ## type2 { type1 a; type2 b; }; printf("%20s %3lu %3lu %3lu\n",#type1 "_" #type2,sizeof(type1),sizeof(type2),sizeof(struct p ## type1## _ ##type2));

#define s(p,type1)    \
  ss(p,type1,char);  \
  ss(p,type1,short);  \
  ss(p,type1,int);    \
  ss(p,type1,long);   \
  ss(p,type1,float);  \
  ss(p,type1,double); \

  s(_,char);
  s(__,short);
  s(___,int);
  s(____,long);
  s(_____,float);
  s(______,double);
}

int main (int argc, const char ** argv)
{
  //  check_sizes();

  if ( argc != 2 ) {
    printf("Must give size of array!\n");
    exit(1);
  }

  unsigned long long size = atoll(argv[1]);


  test_radix (size);

}
