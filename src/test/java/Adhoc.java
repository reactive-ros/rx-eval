import org.junit.Test;
import org.rhea_core.Stream;
import org.rhea_core.util.functions.Actions;
import org.rhea_core.util.functions.Func3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jm.constants.Durations;
import jm.constants.Instruments;
import jm.music.data.Note;
import jm.music.data.Part;
import jm.music.data.Phrase;
import jm.music.data.Rest;
import jm.util.Play;
import jm.util.Write;
import rx_eval.RxjavaEvaluationStrategy;
import test_data.utilities.Threads;

import static jm.constants.Durations.*;
import static jm.constants.Pitches.*;

//import jm.music.data.Rest;

/**
 * @author Orestis Melkonian
 */
public class Adhoc {

    static int bound(int value, int min, int max) {
        if (value > max) return max;
        if (value < min) return min;
        return value;
    }

//    @Test
    public void jmusic() {
        Stream.evaluationStrategy = new RxjavaEvaluationStrategy();

        Func3<Double, Double, Integer, Stream<Part>> chaoticGen = (A, B, type) ->
                Stream.using(
                () -> {
                    Map<String, Double> ret = new HashMap<>();
                    ret.put("a", A);
                    ret.put("b", B);
                    ret.put("xOld", 0.0);
                    ret.put("yOld", 0.0);
                    return ret;
                },
                map -> Stream
                        .range(0, 100)
                        .map(i -> {
                            if (Math.random() > 0.7)
                                return new Rest(SN);

                            double a = map.get("a");
                            double b = map.get("b");
                            double xOld = map.get("xOld");
                            double yOld = map.get("yOld");
                            double x = (1 + yOld) - (a * xOld * xOld);
                            double y = b * xOld;

                            int pitch = bound((int)(x*36)+48, E2, E6);
                            Note note = new Note(pitch, SN);
                            map.replace("xOld", x);
                            map.replace("yOld", y);

                            return note;
                        })
                        .collect(Phrase::new, Phrase::add)
                        .map(phrase -> new Part("inst" + type, type, phrase))
                ,
                Actions.EMPTY
        );

//        Stream<Part> guitar = chaoticGen.call(1.4, 0.3, Instruments.JAZZ_ORGAN);
        Stream<Part> drums = chaoticGen.call(1.04, 0.3, Instruments.SLAP_BASS);

        drums.subscribe(Play::midi);

//        Stream<Score> scores = Stream.zip(guitar, drums, (g, d) -> new Score(new Part[] {g, d}));
//        scores.subscribe(Play::midi);


        Threads.sleep();
    }

//    @Test
    public void twelveTone() {
        Stream.evaluationStrategy = new RxjavaEvaluationStrategy();


        // Pitches
        Stream<Note[]> pitches =
//                gen(4)
                gen(1).repeat(4)
                .map(TwelveTone::instantiate);

        // Rhythms
        Stream<Note[]> rhythms =
                gen(4)
//                gen(1).repeat(4)
                .map(TwelveTone::instantiateRhythm);


        Stream.zip(pitches, rhythms, (p, r) -> {
            for (int i = 0; i < 12; i++) {
                Note n = p[i];
                n.setRhythmValue(r[i].getRhythmValue());
            }
            return p;
        })
        // Conversion
        .collect(Phrase::new, Phrase::addNoteList)
        .collect(Part::new, Part::addPhrase)

        // Set tempo
        .doOnNext(part -> part.setTempo(240))

        // Output
//        .subscribe(Play::midi);
        .subscribe(part -> Write.midi(part, "12tone.mid"));
//        .subscribe(View::show);


        Threads.sleep();
    }

    Stream<TwelveTone> gen(int n) {
        List<TwelveTone> streams = new ArrayList<>();
        for (int i = 0; i < n; i++)
            streams.add(new TwelveTone());
        return Stream
                .from(streams)
                .doOnNext(tt -> {
                    double rand = Math.random();
                    if (rand > 0.666)
                        tt.invert();
                    else if (rand > 0.333)
                        tt.retrograde();
                    else
                        tt.invert_retrograde();
                });
    }

    class TwelveTone {
        List<Integer> tones;

        public TwelveTone() {
            tones = generate12Tone();
        }

        public void invert() {
            tones = tones.parallelStream().map(i -> (12 - i) % 12).collect(Collectors.toList());
        }

        public void retrograde() {
            Collections.reverse(tones);
        }

        public void invert_retrograde() {
            invert();
            retrograde();
        }

        public Note[] instantiate() {
            return tones
                    .stream()
                    .map(Note::getNote)
                    .map(string -> new Note(new Note(string).getPitchValue(), SN))
                    .toArray(Note[]::new);
        }

        public Note[] instantiateRhythm() {
            return tones
                    .stream()
                    .map(Adhoc::getRhythmValue)
                    .map(rhythm -> new Note(C3, rhythm))
                    .toArray(Note[]::new);
        }
    }

    static List<Integer> generate12Tone() {
        List<Integer> tones = Arrays.asList(0,1,2,3,4,5,6,7,8,9,10,11);
        // Random permutation
        Collections.shuffle(tones);
        return tones;
    }

    static double getRhythmValue(int tone) {
        switch (tone) {
            case 0: return EN;
            case 1: return QN;
            case 2: return DQN;
            case 3: return C;
            case 4: return QN + EN;
            case 5: return DQN;
            case 6: return DDQN;
            case 7: return HN;
            case 8: return HN + EN;
            case 9: return HN + QN;
            case 10: return HN + DQN;
            case 11: return DHN;
        }
        return C;
    }
}
