/*
 * (c) copyright 1987 by the Vrije Universiteit, Amsterdam, The Netherlands.
 * See the copyright notice in the ACK home directory, in the file "Copyright".
 */
/* $Header: /mn/sadir/u1/steinhh/cvs/bitmap/src/QSORT.cc,v 1.1.1.1 2009/06/25 13:29:10 steinhh Exp $ */

#include <stdlib.h>
#include "QSORT.h"

static void qsort1(char *, char *, size_t, QCOMPAR_char, const void *);
static void qexchange(char *, char *, size_t);
static void q3exchange(char *, char *, char *, size_t);

void QSORT(void *base, size_t nel, size_t width,
           int (*compar)(const void *, const void *, const void *), const void *extra)
{
        QCOMPAR_char qcompar;
        /* when nel is 0, the expression '(nel - 1) * width' is wrong */
        if (!nel)
                return;
        qcompar = (QCOMPAR_char)compar;
        qsort1((char *)base, (char *)base + (nel - 1) * width, width, qcompar, extra);
}

static void
qsort1(char *a1, char *a2, size_t width, QCOMPAR_char qcompar, const void *extra)
{
        char *left, *right;
        char *lefteq, *righteq;
        int cmp;

        for (;;)
        {
                if (a2 <= a1)
                        return;
                left = a1;
                right = a2;
                lefteq = righteq = a1 + width * (((a2 - a1) + width) / (2 * width));
                /*
                   Pick an element in the middle of the array.
                   We will collect the equals around it.
                   "lefteq" and "righteq" indicate the left and right
                   bounds of the equals respectively.
                   Smaller elements end up left of it, larger elements end
                   up right of it.
                */
        again:
                while (left < lefteq && (cmp = (*qcompar)(left, lefteq, extra)) <= 0)
                {
                        if (cmp < 0)
                        {
                                /* leave it where it is */
                                left += width;
                        }
                        else
                        {
                                /* equal, so exchange with the element to
                                   the left of the "equal"-interval.
                                */
                                lefteq -= width;
                                qexchange(left, lefteq, width);
                        }
                }
                while (right > righteq)
                {
                        if ((cmp = (*qcompar)(right, righteq, extra)) < 0)
                        {
                                /* smaller, should go to left part
                                 */
                                if (left < lefteq)
                                {
                                        /* yes, we had a larger one at the
                                           left, so we can just exchange
                                        */
                                        qexchange(left, right, width);
                                        left += width;
                                        right -= width;
                                        goto again;
                                }
                                /* no more room at the left part, so we
                                   move the "equal-interval" one place to the
                                   right, and the smaller element to the
                                   left of it.
                                   This is best expressed as a three-way
                                   exchange.
                                */
                                righteq += width;
                                q3exchange(left, righteq, right, width);
                                lefteq += width;
                                left = lefteq;
                        }
                        else if (cmp == 0)
                        {
                                /* equal, so exchange with the element to
                                   the right of the "equal-interval"
                                */
                                righteq += width;
                                qexchange(right, righteq, width);
                        }
                        else /* just leave it */
                                right -= width;
                }
                if (left < lefteq)
                {
                        /* larger element to the left, but no more room,
                           so move the "equal-interval" one place to the
                           left, and the larger element to the right
                           of it.
                        */
                        lefteq -= width;
                        q3exchange(right, lefteq, left, width);
                        righteq -= width;
                        right = righteq;
                        goto again;
                }
                /* now sort the "smaller" part */
                qsort1(a1, lefteq - width, width, qcompar, extra);
                /* and now the larger, saving a subroutine call
                   because of the for(;;)
                */
                a1 = righteq + width;
        }
        /*NOTREACHED*/
}

static void
qexchange(char *p, char *q,
          size_t n)
{
        int c;

        while (n-- > 0)
        {
                c = *p;
                *p++ = *q;
                *q++ = c;
        }
}

static void
q3exchange(char *p, char *q, char *r,
           size_t n)
{
        int c;

        while (n-- > 0)
        {
                c = *p;
                *p++ = *r;
                *r++ = *q;
                *q++ = c;
        }
}
