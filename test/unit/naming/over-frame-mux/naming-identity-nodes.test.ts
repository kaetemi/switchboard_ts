/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The node suite `../naming-identity-nodes.test.ts` again, over the frame
// mux (`FrameMuxConnection`) instead of the toy mux.

import { useMux } from '../../../support/node/harness.ts';

useMux('frame');
await import('../naming-identity-nodes.test.ts');
