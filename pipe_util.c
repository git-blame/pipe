/* pipe_util.c - The implementation for experimental pipe extensions.
 *
 * The MIT License
 * Copyright (c) 2011 Clark Gaebel <cg.wowus.cg@gmail.com>
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include "pipe_util.h"

#include <assert.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32 // use the native win32 API on Windows

#include <windows.h>

#define thread_create(f, p) \
        CreateThread(NULL,                          \
                     0,                             \
                     (LPTHREAD_START_ROUTINE)(f),   \
                     (p),                           \
                     0,                             \
                     NULL)

static inline void thread_destroy(THREAD_HANDLE t)
{
    CloseHandle(t);
}

#else // fall back on pthreads

#include <pthread.h>

static inline THREAD_HANDLE thread_create(void *(*f) (void*), void* p)
{
    pthread_t t;
    pthread_create(&t, NULL, f, p);
    return t;
}

static inline void thread_destroy(THREAD_HANDLE t)
{
    pthread_join(t, NULL);
}

#endif

pipeline_t pipe_trivial_pipeline(pipe_t* p)
{
    return (pipeline_t) {
        .in  = pipe_producer_new(p),
        .out = pipe_consumer_new(p)
    };
}

#define DEFAULT_BUFFER_SIZE     128

typedef struct {
    pipe_consumer_t* in;
    pipe_processor_t proc;
    void* aux;
    pipe_producer_t* out;
} connect_data_t;

static void* process_pipe(void* param)
{
    connect_data_t p = *(connect_data_t*)param;
    free(param);

    char* buf = malloc(DEFAULT_BUFFER_SIZE * pipe_elem_size(PIPE_GENERIC(p.in)));

    size_t elems_read;

    while((elems_read = pipe_pop(p.in, buf, DEFAULT_BUFFER_SIZE)))
        p.proc(buf, elems_read, p.out, p.aux);

    p.proc(NULL, 0, NULL, p.aux);

    free(buf);

    pipe_consumer_free(p.in);
    pipe_producer_free(p.out);

    return NULL;
}

void pipe_connect(pipe_consumer_t* in,
                  pipe_processor_t proc, void* aux,
                  pipe_producer_t* out,
                  THREAD_HANDLE*   handle)
{
    assert(in);
    assert(out);
    assert(proc);

    connect_data_t* d = malloc(sizeof *d);

    *d = (connect_data_t) {
        .in = in,
        .proc = proc,
        .aux = aux,
        .out = out
    };

    THREAD_HANDLE t = thread_create(&process_pipe, d);
    if(handle != NULL)
      *handle = t;
}

void pipe_connect_free(THREAD_HANDLE handle)
{
    if (handle)
      thread_destroy(handle);
}

pipeline_t pipe_parallel(size_t           instances,
                         THREAD_HANDLE**  handles,
                         size_t           in_size,
                         pipe_processor_t proc,
                         void*            aux,
                         size_t           out_size)
{
    pipe_t* in  = pipe_new(in_size,  0),
          * out = pipe_new(out_size, 0);

    THREAD_HANDLE* threads = malloc(instances * sizeof(*threads));
    int   count   = 0;

    while(instances--)
    {
        THREAD_HANDLE thread  = (THREAD_HANDLE)NULL;
        pipe_connect(pipe_consumer_new(in),
                     proc, aux,
                     pipe_producer_new(out), &thread);
        memcpy((char *)threads+(count++ * sizeof(thread)), &thread, sizeof(thread));
    }

    pipeline_t ret = {
        .in  = pipe_producer_new(in),
        .out = pipe_consumer_new(out)
    };

    pipe_free(in);
    pipe_free(out);

    if(handles != NULL)
        *handles = threads;
    else
        free(threads);
    return ret;
}

static pipeline_t va_pipe_pipeline(int* count,
                                   THREAD_HANDLE** handles,
                                   pipeline_t result_so_far,
                                   va_list args)
{
    pipe_processor_t proc = va_arg(args, pipe_processor_t);

    if(proc == NULL)
        return result_so_far;

    void*  aux       = va_arg(args, void*);
    size_t pipe_size = va_arg(args, size_t);

    if(pipe_size == 0)
    {
        pipe_consumer_free(result_so_far.out);
        result_so_far.out = NULL;
        return result_so_far;
    }

    pipe_t* pipe = pipe_new(pipe_size, 0);

    THREAD_HANDLE handle = (THREAD_HANDLE)NULL;
    pipe_connect(result_so_far.out , proc, aux, pipe_producer_new(pipe), &handle);
    if(count != NULL && handles != NULL)
    {
        int new_count = *count + 1;
        int new_size = new_count * sizeof(*handles);
        THREAD_HANDLE* new_handles = realloc(*handles, new_size);
        if(new_handles != NULL)
        {
            memcpy((char *)new_handles+(*count * sizeof(handle)), &handle, sizeof(handle));
            *count = new_count;
            *handles = new_handles;
        }
    }
    result_so_far.out = pipe_consumer_new(pipe);

    pipe_free(pipe);

    return va_pipe_pipeline(count, handles, result_so_far, args);
}

pipeline_t pipe_pipeline(int* count, THREAD_HANDLE** handles, size_t first_size, ...)
{
    va_list va;
    va_start(va, first_size);

    pipe_t* p = pipe_new(first_size, 0);

    pipeline_t ret = va_pipe_pipeline(count, handles, pipe_trivial_pipeline(p), va);

    pipe_free(p);

    va_end(va);

    return ret;
}

void pipe_handles_free(int count, THREAD_HANDLE** handles)
{
    if(!count || !handles)
        return;
    THREAD_HANDLE* t = *handles;
    for(int i = 0; i < count; i++)
    {
        THREAD_HANDLE handle;
        memcpy(&handle, (char *)t+(i * sizeof(handle)), sizeof(handle));
        pipe_connect_free(handle);
    }
    free(*handles);
    *handles = NULL;
}

/* vim: set et ts=4 sw=4 softtabstop=4 textwidth=80: */
