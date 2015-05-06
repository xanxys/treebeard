package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

type WordEntry struct {
	EntryName string
	Vector    []float32
}

/*
const long long max_size = 2000;         // max length of strings
const long long N = 40;                  // number of closest words that will be shown
const long long max_w = 50;              // max length of vocabulary entries

int main(int argc, char **argv) {
  FILE *f;
  char st1[max_size];
  char *bestw[N];
  char file_name[max_size], st[100][max_size];
  float dist, len, bestd[N], vec[max_size];
  long long words, size, a, b, c, d, cn, bi[100];
  char ch;
  float *M;
  char *vocab;
  if (argc < 2) {
    printf("Usage: ./distance <FILE>\nwhere FILE contains word projections in the BINARY FORMAT\n");
    return 0;
  }
  strcpy(file_name, argv[1]);
  f = fopen(file_name, "rb");
  if (f == NULL) {
    printf("Input file not found\n");
    return -1;
  }
  fscanf(f, "%lld", &words);
  fscanf(f, "%lld", &size);
  vocab = (char *)malloc((long long)words * max_w * sizeof(char));
  for (a = 0; a < N; a++) bestw[a] = (char *)malloc(max_size * sizeof(char));
  M = (float *)malloc((long long)words * (long long)size * sizeof(float));
  if (M == NULL) {
    printf("Cannot allocate memory: %lld MB    %lld  %lld\n", (long long)words * size * sizeof(float) / 1048576, words, size);
    return -1;
  }
  for (b = 0; b < words; b++) {
    a = 0;
    while (1) {
      vocab[b * max_w + a] = fgetc(f);
      if (feof(f) || (vocab[b * max_w + a] == ' ')) break;
      if ((a < max_w) && (vocab[b * max_w + a] != '\n')) a++;
    }
    vocab[b * max_w + a] = 0;
    for (a = 0; a < size; a++) fread(&M[a + b * size], sizeof(float), 1, f);
    len = 0;
    for (a = 0; a < size; a++) len += M[a + b * size] * M[a + b * size];
    len = sqrt(len);
    for (a = 0; a < size; a++) M[a + b * size] /= len;
  }
  fclose(f);
*/

func LoadEntries(rRaw io.Reader) []WordEntry {
	r := bufio.NewReader(rRaw)

	// Reader text header (the first line)
	header, err := r.ReadBytes('\n')
	if err != nil {
		log.Fatal(err)
	}
	headerVals := strings.Split(strings.Trim(string(header), "\n"), " ")
	if len(headerVals) != 2 {
		log.Fatal("Header requires two numbers")
	}
	numWords, _ := strconv.Atoi(headerVals[0])
	numDim, _ := strconv.Atoi(headerVals[1])
	log.Printf("#words=%d / #dim=%d\n", numWords, numDim)

	entries := make([]WordEntry, 0)
	for i := 0; i < numWords; i++ {
		vocabRaw, _ := r.ReadBytes(' ')
		vocab := string(vocabRaw)

		vector := make([]float32, numDim)
		binary.Read(r, binary.LittleEndian, vector)

		entries = append(entries, WordEntry{
			EntryName: vocab,
			Vector:    vector,
		})
	}

	log.Printf("Loaded %d entries\n", len(entries))
	return entries
}

func main() {
	flagInput := flag.String("input", "", "Input path of trained word vectors")
	flagOutput := flag.String("output", "", "Line-delimited jsons of all words with vectors")
	flag.Parse()

	file, err := os.Open(*flagInput)
	if err != nil {
		log.Fatal(err)
	}

	LoadEntries(file)

	log.Println(*flagOutput)

}
