// TYPECODE definition:
// Table (index, value) <-> TYPECODE isv
//
// i = index-size (1, 2, 4, 8)
// s = value-is-signed-flag (0: no; 1: yes)
// v = value-size-type (1, 2, 4, 5[float], 8, 9[double])

// MACROS TO SIMPLIFY E.G.::::::::::::::::::::::::::::::::::::::

// switch (typecode) {
//    case 101: b.b101 = new ha_scalarray_toolbox<uchar8,uchar8>(...);
//   :
//   case 819: b.b819 = new ha_scalarray_toolbox<uint64,sint64>(...);
//   default : ouch...
// }
//
// into:
//
// #define TYPECODE_EXPANSION(itype,vtype,typecode,extra)
//   case typecode: b.b##typecode new ha_scalarray_toolbox<itype,vtype>(...);
//
// #include "typecode_expansion.h"
//
// switch (typecode) {
//    ALL_TYPECODE_EXPANSIONS
//    default: ouch...
// }
//
//
// or
//
// union {
//   class ha_scalarray_toolbox<uchar8,uchar8> b101;
//   :
//   class ha_scalarray_toolbox<uint64,sdoubl> b819;
// } b;

// into:

// or:
//
// union {
// #  define TYPECODE_EXPANSION(itype,vtype,typecode)
//    class ha_scalarray_toolbox<itype,vtype> *b##typecode; // Simple!
//
// #  include "typecode_expansion.h"
//
//    ALL_TYPECODE_EXPANSIONS
// } b;
//
// Or, e.g.:
// #define TYPECODE_EXPANSION(itype,vtype,typecode)
//    case typecode: b.b##typecode->method(...)

// REQUIRED TYPEDEFS:::::::::::::::::::::::::::::::::::::::::::::::::

#ifndef TYPECODE_EXPANSION_TYPEDEFS
typedef uint8 uchar8;
typedef char schar8;
typedef int16 sint16;
//             uint16 already defined [?]
typedef int32 sint32;
//             uint32 already defined [?]
typedef int64 sint64;
//             uint64 already defined [?]
typedef float sfloat;
typedef double sdoubl;

#define TYPECODE_EXPANSION_TYPEDEFS
#endif

// MACRO DEFINITIONS::::::::::::::::::::::::::::::::::::::::::::::::

// Actually, first macro *undefinitions*
#ifdef sTYPECODE_EXPANSION
#undef sTYPECODE_EXPANSION
#undef vTYPECODE_EXPANSION
#undef ALL_TYPECODE_EXPANSIONS
#endif

// Generate signed/unsigned pairs [note: index type always unsigned!!]
// vtype is char8/int16/...
#define sTYPECODE_EXPANSION(itype, vtype, i, v, extra)             \
  TYPECODE_EXPANSION(itype, s##vtype, i##1##v, extra) /* signed */ \
  TYPECODE_EXPANSION(itype, u##vtype, i##0##v, extra) /* unsigned */

// Branch over all value types:
#define vTYPECODE_EXPANSION(itype, i, extra)                      \
  sTYPECODE_EXPANSION(itype, char8, i, 1, extra)                  \
      sTYPECODE_EXPANSION(itype, int16, i, 2, extra)              \
          sTYPECODE_EXPANSION(itype, int32, i, 4, extra)          \
              sTYPECODE_EXPANSION(itype, int64, i, 8, extra)      \
                  TYPECODE_EXPANSION(itype, sfloat, i##15, extra) \
                      TYPECODE_EXPANSION(itype, sdoubl, i##19, extra)

// And here we branch out over all index types
#define ALL_TYPECODE_EXPANSIONS(extra)          \
  vTYPECODE_EXPANSION(uchar8, 1, extra)         \
      vTYPECODE_EXPANSION(uint16, 2, extra)     \
          vTYPECODE_EXPANSION(uint32, 4, extra) \
              vTYPECODE_EXPANSION(uint64, 8, extra)
