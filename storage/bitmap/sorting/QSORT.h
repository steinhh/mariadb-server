typedef int (*QCOMPAR_char)(const char *, const char *,const void *);
typedef int (*QCOMPAR_void)(const void *, const void *,const void *);

void QSORT(void *base, size_t nel, size_t width,
	   QCOMPAR_void compar,const void *extra);
