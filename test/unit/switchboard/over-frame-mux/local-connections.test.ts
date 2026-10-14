/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The node suite `../local-connections.test.ts` again, over the frame mux
// (`FrameMuxConnection`) instead of the toy mux.

import { useMux } from '../../../support/node/harness.ts';

useMux('frame');
await import('../local-connections.test.ts');
